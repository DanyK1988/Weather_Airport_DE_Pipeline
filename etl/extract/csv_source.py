import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)

def extract_csv(path: str)-> pd.DataFrame:
    # Конвертируем строку в объект Path для удобной работы с файловой системой
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Файл не найден: {path}")

    # Перебираем кодировки по очереди: сначала универсальная, потом WIndiows, потом латиница
    for encoding in ('utf-8', 'cp1251', 'latin1'):
        try:
            df = pd.read_csv(path, sep=',', encoding=encoding, quotechar='"', engine="python")
            logger.info(f"Успешно прочитал {path.name} с кодировкой {encoding}")
            return df
        except UnicodeDecodeError:
            logger.warning(f"Ошибка чтения {encoding} кодировкой")

    # Если ни одна кодировка не подошла - бросаем понятную ошибку
    raise ValueError(f"Не могу прочитать файл {path.name} доступными кодировками")