import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

def extract_excel(path: str, sheet_name=0) -> pd.DataFrame:
    # Конвертруем строку в объект Path для удобной работы с файловой системой
    path = Path(path)

    # Проверяем, что файл существует 
    if not path.exists():
        raise FileNotFoundError(f"Файл не найден: {path}")

    # Проверяем расширение - принимаем только .xls и .xlsx
    if path.suffix.lower() not in (".xls", ".xlsx"):
        raise ValueError(f"Формат {path.suffix.lower()} не поддерживается")

    try:
        # sheet_name=0 по умолчанию — читаем первый лист
        # можно передать название листа строкой, например sheet_name="Данные"

        df = pd.read_excel(path, sheet_name=sheet_name)
        logger.info(f"Файл {path.name} успешно прочитан, страница - {sheet_name}")
        return df

    except Exception as e:
        # Логируем ошибку и пробрасываем ее дальше 
        logger.error(f"Не получилось прочитать файл {path.name}: {e}")
        raise