import os
import pathlib
import sys

from dotenv import load_dotenv
from supabase import Client, create_client

from core.backup import backup_table_via_api
from core.convert_to_pd import convert_to_pd, restore_texts_for_llm
from core.logger import log_wide_df_head, logger

load_dotenv()

supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(supabase_url, supabase_key)  # type: ignore

filename = "novaya_rows_api.csv"


def main() -> None:
    try:
        if not supabase_url or not supabase_key:
            logger.error("❌ SUPABASE_URL или SUPABASE_KEY не найдены в .env")
            return

        if not pathlib.Path(filename).exists():
            backup_table_via_api(supabase_client=supabase, table_name="novaya", output_file=filename)
        else:
            logger.info(f"📁 Файл '{filename}' уже существует.")

        # # 2. Читаем файл в Pandas
        df = convert_to_pd(filename)
        log_wide_df_head(df, n=5)

        restore_texts_for_llm(input_csv=filename, output_jsonl="restored_texts.jsonl")

    except KeyboardInterrupt:
        logger.info("⏹️ Операция прервана пользователем")
        sys.exit(0)
    except Exception as e:
        logger.exception(f"❌ Критическая ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
