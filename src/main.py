import os
import pathlib
import sys

import pandas as pd  # pyright: ignore[reportMissingModuleSource]
from dotenv import load_dotenv
from supabase import Client, create_client

from core.backup import backup_table_via_api
from core.logger import logger

load_dotenv()

supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(supabase_url, supabase_key)


def main() -> None:
    try:
        if not supabase_url or not supabase_key:
            logger.error("❌ SUPABASE_URL или SUPABASE_KEY не найдены в .env")
            return

        filename = "novaya_rows_api.csv"

        if not pathlib.Path(filename).exists():
            backup_table_via_api(supabase_client=supabase, table_name="novaya", output_file=filename)
        else:
            logger.info(f"📁 Файл '{filename}' уже существует.")

        # 2. Читаем файл в Pandas
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
            print("--- ПЕРВЫЕ 5 СТРОК ---")
            print(df.head())
        else:
            logger.warning("Файл бэкапа не был создан.")

    except KeyboardInterrupt:
        logger.info("⏹️ Операция прервана пользователем")
        sys.exit(0)
    except Exception as e:
        logger.exception(f"❌ Критическая ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
