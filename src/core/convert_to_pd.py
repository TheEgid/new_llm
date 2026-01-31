import csv
import json
import pathlib
from typing import Any, Dict

import pandas as pd

from core.logger import logger


def convert_to_pd(filename: str) -> pd.DataFrame:
    """
    Loads raw CSV backup into a normalized pandas DataFrame.
    Handles multiline text, embeddings and malformed JSON.
    """
    path = pathlib.Path(filename)
    if not path.exists():
        logger.warning("Файл бэкапа не был создан.")
        return pd.DataFrame()

    df = pd.read_csv(
        filename,
        sep=",",
        quoting=csv.QUOTE_ALL,
        escapechar="\\",
    )

    if "created_at" in df.columns:
        df["created_at"] = (
            pd.to_datetime(df["created_at"], errors="coerce")
            .dt.date
        )
        logger.info("📅 Дата приведена к формату YYYY-MM-DD")

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].map(
            lambda x: x.strip() if isinstance(x, str) else x
        )

    if "content" in df.columns:
        df["content"] = df["content"].replace(
            r"[\n\r\t]+", " ", regex=True
        )

    logger.info(f"✅ Данные успешно загружены. Строк: {len(df)}")
    return df


def restore_texts_for_llm(input_csv: str, output_jsonl: str) -> pd.DataFrame:
    """
    Restores chunked documents into full texts grouped by source URL
    and exports them into JSONL for LLM ingestion.
    """
    df = convert_to_pd(input_csv)
    if df.empty:
        return df

    def parse_metadata(value: Any) -> Dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str) and value.strip():
            try:
                return json.loads(value.replace("'", '"'))
            except json.JSONDecodeError:
                return {}
        return {}

    df["metadata"] = df["metadata"].fillna("{}").apply(parse_metadata)
    df["url"] = df["metadata"].apply(lambda m: m.get("url", ""))

    df["content"] = (
        df["content"]
        .fillna("")
        .astype(str)
        .replace(r"[\n\r\t]+", " ", regex=True)
        .str.strip()
    )

    sort_cols = ["url"]
    if "chunk_index" in df.columns:
        sort_cols.append("chunk_index")
    elif "id" in df.columns:
        sort_cols.append("id")

    df = df.sort_values(sort_cols)

    def reassemble(group: pd.DataFrame) -> pd.Series:
        return pd.Series(
            {
                "text": " ".join(group["content"].tolist()),
                "source_url": group.name,
                "date": group["created_at"].iloc[0]
                if "created_at" in group.columns
                else None,
            }
        )

    df = df[df["url"].astype(bool)]
    restored_df = (
        df.groupby("url", dropna=True)
        .apply(reassemble, include_groups=False)  # type: ignore
        .reset_index(drop=True)
    )

    restored_df.to_json(
        output_jsonl,
        orient="records",
        lines=True,
        force_ascii=False,
    )

    logger.info(f"✅ Восстановлено статей: {len(restored_df)}")
    return restored_df
