import json
import pathlib
from typing import Any

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
    df = pd.read_csv(input_csv)

    df['content'] = df['content'].fillna('').astype(str)
    df['content'] = df['content'].replace(r'[\n\r\t]+', ' ', regex=True).str.strip()

    # 3. Парсинг метаданных (если они записаны как строка)
    def parse_metadata(x: str) -> dict:
        if isinstance(x, str):
            try:
                # Заменяем одинарные кавычки на двойные для корректного JSON
                return json.loads(x.replace("'", '"'))
            except:  # noqa: E722
                return {}
        return x

    df['metadata'] = df['metadata'].apply(parse_metadata)

    def reassemble_chunks(group: pd.DataFrame) -> Any:  # noqa: ANN401
        # Сортируем части статьи по их порядку в метаданных
        sorted_group = group.sort_values(by='id')  # Или по метаданным, если id одинаковый
        full_text = " ".join(sorted_group['content'].tolist())

        # Берем метаданные от первой части
        meta = sorted_group['metadata'].iloc[0]

        return pd.Series({
            'text': full_text,
            'source_url': meta.get('url', ''),
            'date': sorted_group['created_at'].iloc[0]
        })

    # Если id уникален для статьи, а не для чанка — используем его
    restored_df = df.groupby('id').apply(reassemble_chunks).reset_index()

    # 5. Сохранение в JSONL (лучший формат для LLM - по одной записи на строку)
    restored_df.to_json(output_jsonl, orient='records', lines=True, force_ascii=False)

    print(f"✅ Восстановлено статей: {len(restored_df)}")
    return restored_df
