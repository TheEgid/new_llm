import sys

from core import logger


def main() -> None:
    try:
        print("month_name2")

    except KeyboardInterrupt:

        sys.exit(0)
    except Exception as e:
        logger.exception(f"❌ Критическая ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
