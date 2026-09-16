import logging
import os

from etl.landing.minio_client import (
    get_minio_client,
    list_raw_batches,
    read_raw_batch,
)

from db import get_engine
from etl.load.aerodatabox_api_load import (
    load_aerodatabox_raw,
)


logger = logging.getLogger(__name__)


def main():
    # Получаем клиент для подключения к MinIO
    minio_client = get_minio_client()

    # Получаем префикс из переменной окружения
    prefix = os.getenv("MINIO_PREFIX", "")

    # Получаем список объектов, соответствующих префиксу
    object_names = list_raw_batches(minio_client, prefix=prefix)

    if not object_names:
        logger.error(f"Не найдено ни одного объекта по префиксу '{prefix}'")
        return

    logger.info(f"Найдено объектов для переобработки: {len(object_names)}")

    # Создаём подключение к PostgreSQL один раз перед циклом
    engine = get_engine()

    loaded_count = 0
    for object_name in object_names:
        batch = read_raw_batch(minio_client, object_name)

        if batch is None:
            logger.error(f"Пропускаем {object_name}: не удалось прочитать объект")
            continue

        # load_aerodatabox_raw() ожидает список, поэтому передаём прочитанный batch как один элемент списка
        load_aerodatabox_raw([batch], engine)
        loaded_count += 1

    logger.info(f"Переобработка завершена: обработано объектов {loaded_count} из {len(object_names)}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()