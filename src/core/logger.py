import logging
import sys


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
