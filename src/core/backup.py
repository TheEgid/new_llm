import csv
import json
import time
from pathlib import Path
from typing import Any

import supabase

from core.logger import logger


def _serialize_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def backup_table_via_api(
    table_name: str,
    output_file: str,
    supabase_client: supabase.Client,
) -> None:
    """
    Creates a safe CSV backup of a Supabase table.
    Guarantees CSV validity for multiline text, JSON and embeddings.
    """
    try:
        logger.info(f"⏳ Получение данных из таблицы {table_name} через API...")

        count_response = (
            supabase_client
            .table(table_name)
            .select("*", count="exact")  # type: ignore
            .execute()
        )
        total_rows = count_response.count or 0

        logger.info(f"📊 Таблица '{table_name}' содержит {total_rows} строк")

        if total_rows == 0:
            logger.warning("⚠️ Таблица пуста. Бэкап не требуется.")
            return

        if input(f"Продолжить бэкап {total_rows} строк? (y/n): ").lower() != "y":
            logger.info("❌ Бэкап отменен пользователем")
            return

        all_data: list[dict] = []
        page_size = 1000
        offset = 0

        while True:
            logger.info(f"📥 Загрузка записей {offset + 1}-{offset + page_size}...")

            response = (
                supabase_client
                .table(table_name)
                .select("*")
                .order("id")
                .range(offset, offset + page_size - 1)
                .execute()
            )

            page_data = response.data or []
            if not page_data:
                break

            all_data.extend(page_data)  # type: ignore
            offset += page_size

            if len(page_data) < page_size:
                break

            time.sleep(0.1)

        if not all_data:
            logger.warning("⚠️ Данные не получены.")
            return

        headers = list(all_data[0].keys())

        with Path(output_file).open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=headers,
                quoting=csv.QUOTE_ALL,
                escapechar="\\",
            )
            writer.writeheader()

            for row in all_data:
                safe_row = {
                    key: _serialize_value(value)
                    for key, value in row.items()
                }
                writer.writerow(safe_row)

        logger.info(f"✅ Бэкап сохранен: {output_file} (строк: {len(all_data)})")

    except Exception:
        logger.exception("❌ Ошибка при создании бэкапа")
        raise
