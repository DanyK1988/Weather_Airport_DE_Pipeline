import os
import json
import logging
from io import BytesIO
from datetime import datetime, timezone

from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

BUCKET_NAME = "flight-raw-landing"


def get_minio_client():
    return Minio(
        "minio:9000",
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False,
    )


def save_raw_batch(client, batch: dict):
    """Сохраняет один сырой батч как JSON-файл в MinIO перед его загрузкой в PostgreSQL."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    object_name = (
        f"{batch['airport_code']}/"
        f"{batch['from_local'].replace(':', '-')}_"
        f"{timestamp}.json"
    )

    payload_bytes = json.dumps(batch, ensure_ascii=False).encode("utf-8")

    try:
        client.put_object(
            BUCKET_NAME,
            object_name,
            data=BytesIO(payload_bytes),
            length=len(payload_bytes),
            content_type="application/json",
        )
        logger.info(f"Сырой батч сохранён в MinIO: {object_name}")
        return object_name
    except S3Error as e:
        logger.error(f"Не удалось сохранить батч в MinIO ({object_name}): {e}")
        return None


def read_raw_batch(client, object_name: str):
    """Читает ранее сохранённый сырой батч обратно из MinIO по его ключу."""
    response = None
    try:
        response = client.get_object(BUCKET_NAME, object_name)
        raw_bytes = response.read()
        return json.loads(raw_bytes.decode("utf-8"))
    except S3Error as e:
        logger.error(f"Не удалось прочитать объект {object_name} из MinIO: {e}")
        return None
    finally:
        if response is not None:
            response.close()
            response.release_conn()

def list_raw_batches(client, prefix: str = ""):
    """Возвращает список ключей всех объектов в landing zone, соответствующих указанному префиксу."""
    try:
        objects = client.list_objects(BUCKET_NAME, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]
    except S3Error as e:
        logger.error(f"Не удалось получить список объектов с префиксом '{prefix}' из MinIO: {e}")
        return []