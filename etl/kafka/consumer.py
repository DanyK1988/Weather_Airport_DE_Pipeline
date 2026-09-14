import os
import json
import time
import signal
import logging

from kafka import KafkaConsumer, KafkaProducer

from db import get_engine
from etl.load.aerodatabox_api_load import load_aerodatabox_raw


logger = logging.getLogger(__name__)

MAIN_TOPIC = "flight_events"
DLQ_TOPIC = "flight_events.dlq"
GROU_ID = "flight_group"

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = [2, 5, 12]
COMMIT_EVERY_N_MESSAGES = 1


class GracefulShutdown:
    """Ловит SIGTERM/SIGINT, чтобы Consumer успел закоммитить offset перед остановкой."""
    stop_requested = False

    def __init__(self):
        signal.signal(signal.SIGTERM, self._handle)
        signal.signal(signal.SIGINT, self._handle)

    def _handle(self, signum, frame):
        logger.info(f"Получен сигнал {signum}, завершаем работу после тукущего сообщения ...")
        self.stop_requested = True

def process_with_retry(batch, engine):
    """Пытаемся загрузить батч с несколькими попытками и растущими паузами"""
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            load_aerodatabox_raw(data=[batch], engine=engine)
            return True
        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES:
                pause = RETRY_BACKOFF_SECONDS[attempt - 1]
                logger.warning(
                    f"Попытка {attempt}/{MAX_RETRIES} не удалась: {e}. "
                    f"Повтор через {pause} секунду"
                )
                time.sleep(pause)
    logger.error(f"Все {MAX_RETRIES} попытки исчерпаны. Последняя ошибка: {last_error}")
    return False

def send_to_dlq(dlq_producer, batch, error_message):
    """Публикует необработанное сообщение в отдельный топик для последующего разбора."""
    dlq_payload = {
        "original_batch": batch,
        "error": error_message,
    }
    dlq_producer.send(DLQ_TOPIC, value=dlq_payload)
    dlq_producer.flush()
    logger.info(f"Батч {batch.get('from_local')} -> {batch.get('to_local')} отправлен в DLQ")


def main():
    engine = get_engine()
    shutdown = GracefulShutdown()

    consumer = KafkaConsumer(
        MAIN_TOPIC,
        bootstrap_servers="kafka:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=GROU_ID,
    )

    dlq_producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    logger.info("Consumer запущен, ожидание сообщений...")

    processed_since_commit = 0

    try:
        for message in consumer:
            batch = message.value

            success = process_with_retry(batch, engine)

            if success:
                logger.info(
                    f"Загружен батч {batch['from_local']} -> {batch['to_local']}"
                    f"(offset {message.offset})"
                )
            else:
                send_to_dlq(dlq_producer, batch, "Превышено число попыток")

            processed_since_commit +=1
            if processed_since_commit >= COMMIT_EVERY_N_MESSAGES:
                consumer.commit()
                processed_since_commit = 0

            if shutdown.stop_requested:
                break
    finally:
        consumer.commit()
        consumer.close()
        dlq_producer.close()
        logger.info("Consumer остановлен, offset сохранен")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()