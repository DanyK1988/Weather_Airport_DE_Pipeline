import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)

def extract_parquet(path: str) -> pd.DataFrame:

    # преобразуем строку в объект Path для удобной работы с путями
    path = Path(path)

    # Проверяем, что файл существует
    if not path.exists():
        raise FileNotFoundError(f"Файл {path.name} не найден")

    # Проверяем расширение файла
    if path.suffix.lower() != ".parquet":
        raise ValueError(f"Ожидается .parquet тип файла: {path.name}")

    try:
        # логируем начало чтения файла
        logger.info(f"Чтение parquet файла: {path.name}")

        df = pd.read_parquet(path)

        logger.info(f"Загружено {len(df)} строк, {len(df.columns)} колонок")
        return df

    # отдельная обработка ошибки зависимостей (если не установлен pyarrow или fastparquet)
    except ImportError:
        logger.error("Отсутствует зависимость на parquet. Установите 'pyarrow' или 'fastparquet'")
        raise

    # обработка всех остальных ошибок
    except Exception as e:
        logger.error(f"Ошибка при чтении файла {path.name}: {e}")
        raise