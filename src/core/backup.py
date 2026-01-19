import csv
import time
from pathlib import Path

import supabase

from core.logger import logger


def backup_table_via_api(table_name: str, output_file: str, supabase_client: supabase.Client) -> None:
    """
    Создает надежный бэкап таблицы через Supabase API и сохраняет в CSV.
    """
    try:
        logger.info(f"⏳ Получение данных из таблицы {table_name} через API...")

        count_response = supabase_client.table(table_name).select("*", count="exact").execute()
        total_rows = count_response.count or (len(count_response.data) if count_response.data else 0)

        logger.info(f"📊 Таблица '{table_name}' содержит {total_rows} строк")

        if total_rows == 0:
            logger.warning("⚠️ Таблица пуста. Бэкап не требуется.")
            return

        user_input = input(f"Продолжить бэкап {total_rows} строк? (y/n): ")
        if user_input.lower() != 'y':
            logger.info("❌ Бэкап отменен пользователем")
            return

        all_data = []
        page_size = 1000
        offset = 0

        while True:
            logger.info(f"📥 Загрузка записей {offset + 1}-{offset + offset + page_size}...")

            response = supabase_client.table(table_name).select("*").order("id").range(offset, offset + page_size - 1).execute()

            page_data = response.data
            if not page_data:
                break

            all_data.extend(page_data)
            offset += page_size

            if len(page_data) < page_size:
                break
            time.sleep(0.1)

        if all_data:
            headers = list(all_data[0].keys())
            with Path(output_file).open('w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=headers, quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()
                writer.writerows(all_data)

            logger.info(f"✅ Бэкап сохранен: {output_file} (строк: {len(all_data)})")

    except Exception as e:
        logger.error(f"❌ Ошибка API: {e}")
        raise
