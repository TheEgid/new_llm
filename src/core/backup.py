import time
from pathlib import Path

import supabase

from core.logger import logger


def backup_table_via_api(table_name: str, output_file: str, supabase: supabase.Client, delimiter: str = "||||") -> None:
    """
    Создает бэкап таблицы через Supabase API (без прямого доступа к DB)
    """
    try:
        logger.info(f"⏳ Получение данных из таблицы {table_name} через API...")

        count_response = supabase.table(table_name).select("*", count="exact").execute()
        total_rows = count_response.count
        if total_rows is None:
            total_rows = len(count_response.data) if count_response.data else 0

        logger.info(f"📊 Таблица '{table_name}' содержит {total_rows} строк")

        if total_rows == 0:
            logger.warning("⚠️ Таблица пуста. Бэкап не требуется.")
            return

        user_input = input(f"Продолжить бэкап {total_rows} строк? (y/n): ")
        if user_input.lower() != 'y':
            logger.info("❌ Бэкап отменен пользователем")
            return

        all_data = []
        page_size = 1000  # Размер страницы
        offset = 0

        logger.info(f"📥 Начинаем загрузку данных (страницами по {page_size} записей)...")

        while True:
            logger.info(f"⏳ Загрузка записей {offset + 1}-{offset + page_size}...")

            # Получаем данные с пагинацией
            response = supabase.table(table_name).select("*").order("id").range(offset, offset + page_size - 1).execute()

            page_data = response.data
            if not page_data:
                break

            all_data.extend(page_data)
            offset += page_size

            if len(page_data) < page_size:
                break

            time.sleep(0.1)

        logger.info(f"📊 Загружено {len(all_data)} записей из {total_rows}")

        if not all_data:
            logger.warning("⚠️ Данные не найдены.")
            return

        with Path(output_file).open('w', encoding='utf-8') as f:
            headers = list(all_data[0].keys())
            f.write(delimiter.join(headers) + "\n")

            for row in all_data:
                line_values = [str(row.get(h)) if row.get(h) is not None else "" for h in headers]
                f.write(delimiter.join(line_values) + "\n")

        logger.info(f"✅ Бэкап сохранен: {output_file} (строк: {len(all_data)})")

    except Exception as e:
        logger.error(f"❌ Ошибка API: {e}")
        raise
