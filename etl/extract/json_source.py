import logging
from pathlib import Path
import pandas as pd
import json

logger = logging.getLogger(__name__)

def extract_json(path: str, lines: bool = False, record_path: str | None = None) -> pd.DataFrame:

    # Конвертируем строку в объект Path для удобной работы с файловой системой
    path = Path(path)

    # Проверяем, что файл существует
    if not path.exists():
        raise FileNotFoundError(f"Файл не найден: {path}")

    # Проверяем формат файла
    if path.suffix().lower() != ".json":
        raise ValueError(f"Формат {path.suffix.lower()} не поддерживается. Ожидается .json")

    logger.info(f"Начинаем чтение файла {path.name}")

    try:

        # Формат JSON Lines: каждая строка файла - отдельный JSON объект
        if lines:
            df = pd.read_json(path, lines=True)
            if df.empty:
                logger.warning(f"Файл {path.name} не содержит данных")
            logger.info(f"Загружено {len(df)} строк, {len(df.columns)} колонок")
            return df

        # --- Читаем как обычный JSON ---
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

    except json.JSONDecodeError as e:

        # Файл не является валидным JSON
        logger.error(f"Формат файла {path.name} не поддерживается: {e}")
        raise

    except ValueError as e:
        # Ошибка pandas при чтении (например, несовместимая структура) 
        logger.error(f"Ошибка Pandas при чтении файла {path.name}: {e}")
        raise

    except Exception as e:
        # Любые другие ошибка
        logger.error(f"Неожиданная ошибка при чтении файла {path.name}: {e}")
        raise

    # -- Преобразование структуры -- 
    try:
        if isinstance(data, list):

            df = pd.DataFrame(data)

        elif isinstance(data, dict):
            if record_path:
                # Данные лежат внутри конкретного ключа словаря

                if record_path not in data:
                    raise ValueError(f"Ключ {record_path} не найдет в JSON")

            else:

                # Весь словарь - одна записб, разворачиваем в одну строку
                df = pd.json_normalize(data)
        else:

            # Например, если JSON содержит просто число или строку
            raise ValueError(f"Структура JSON файла {path.name} не поддерживается")

        if df.empty:
            logger.warning(f"Файл {path.name} загружен, но не сожержит данные")
        logger.info(f"Загружено {len(df)} строк, {len(df.columns)} колонок")
        return df

    except Exception as e:
        logger.error(f"Не получилось обработать файл {path.name}: {e}")
        raise
    