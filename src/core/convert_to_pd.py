import json
import pathlib
from typing import Any, Dict

import pandas as pd

from core.logger import logger


def convert_to_pd(filename: str) -> None:
    if pathlib.Path(filename).exists():
        df = pd.read_csv(filename)
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce').dt.date
            logger.info("📅 Дата приведена к формату YYYY-MM-DD")
        df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x) if col.dtype == "object" else col)
        if 'content' in df.columns:
            df['content'] = df['content'].replace(r'[\n\r\t]+', ' ', regex=True)
        df = df.sort_values(by='created_at', ascending=False)

        logger.info(f"✅ Данные успешно загружены в DataFrame. Строк: {len(df)}")
    else:
        logger.warning("Файл бэкапа не был создан.")
    return df


def restore_texts_for_llm(input_csv: str, output_jsonl: str) -> pd.DataFrame:
    """
    Reassembles chunked texts into full documents grouped by source URL
    and exports them as JSONL suitable for LLM ingestion.
    """
    df = pd.read_csv(input_csv)

    def parse_metadata(value: Any) -> Dict[str, Any]:  # noqa: ANN401
        if isinstance(value, dict):
            return value
        if isinstance(value, str) and value.strip():
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return {}
        return {}

    df["metadata"] = df["metadata"].apply(parse_metadata)

    df["url"] = df["metadata"].apply(lambda m: m.get("url", ""))

    df["content"] = (
        df["content"]
        .fillna("")
        .astype(str)
        .replace(r"[\n\r\t]+", " ", regex=True)
        .str.strip()
    )

    sort_cols = ["url"]
    if "id" in df.columns:
        sort_cols.append("id")

    df = df.sort_values(sort_cols)

    def reassemble(group: pd.DataFrame) -> pd.Series:
        return pd.Series(
            {
                "text": " ".join(group["content"].tolist()),
                "source_url": group["url"].iloc[0],
                "date": group["created_at"].iloc[0]
                if "created_at" in group.columns
                else None,
            }
        )

    restored_df = (
        df.groupby("url", as_index=False)
        .apply(reassemble)
        .reset_index(drop=True)
    )

    restored_df.to_json(
        output_jsonl,
        orient="records",
        lines=True,
        force_ascii=False,
    )

    print(f"✅ Восстановлено статей: {len(restored_df)}")
    return restored_df
