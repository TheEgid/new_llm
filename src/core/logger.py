import logging
import sys

import pandas as pd


def setup_logger(name: str = "app_logger") -> logging.Logger:
    """Настраивает глобальный логгер для приложения."""

    # 1. Создаем форматтер
    log_format = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-5.5s| %(message)s",
        datefmt="%H:%M:%S"
    )

    # 2. Обработчик для вывода в консоль (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_format)
    console_handler.setLevel(logging.INFO)  # Уровень для хэндлера

    # 3. Настройка логгера
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)  # Уровень для самого логгера

    # 4. ВАЖНО: Проверяем, нет ли уже хэндлеров, чтобы избежать дублирования
    # и ДОБАВЛЯЕМ хэндлер
    if not logger.handlers:
        logger.addHandler(console_handler)

        # log_file_path = Path("logs/app.log")
        # log_file_path.parent.mkdir(exist_ok=True)
        # file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
        # file_handler.setFormatter(log_format)
        # logger.addHandler(file_handler)

    # Запрещаем передачу логов корневому логгеру (чтобы не было двойных строк)
    logger.propagate = False

    return logger


# Создаем экземпляр логгера
logger = setup_logger()


def log_wide_df_head(df: pd.DataFrame, n: int = 5) -> None:
    if df.empty:
        logger.info("DataFrame пуст.")
        return

    if n >= 0:
        subset = df.head(n)
        label = f"первые {n}"
    else:
        subset = df.tail(abs(n))
        label = f"последние {abs(n)}"

    logger.info(f"--- Предпросмотр широких данных ({label} строк) ---")

    max_col_len = max(len(str(col)) for col in df.columns)
    output = []
    for i, row in enumerate(subset.to_dict(orient='records')):
        real_idx = subset.index[i]

        output.append(f"\n=== Запись (индекс {real_idx}) ===")
        for col, val in row.items():
            clean_val = str(val).replace('\n', ' ')
            output.append(f"{str(col).ljust(max_col_len)} : {clean_val}")
        output.append("-" * (max_col_len + 20))
    logger.info("\n".join(output))
