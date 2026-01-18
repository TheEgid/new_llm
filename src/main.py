import os
import sys

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

        backup_table_via_api(supabase=supabase, table_name="novaya", output_file="novaya_rows_api.csv")

    except KeyboardInterrupt:
        logger.info("⏹️ Операция прервана пользователем")
        sys.exit(0)
    except Exception as e:
        logger.exception(f"❌ Критическая ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
