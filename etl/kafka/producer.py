import os
import json
import logging
from datetime import datetime, timedelta

from kafka import KafkaProducer

from etl.extract.aerodatabox_api import extract_aerodatabox_fids_range_batch

logger = logging.getLogger(__name__)


def get_default_dates():
    today = datetime.today()
    end_date = today
    start_date = end_date - timedelta(days=2)
    return (
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )

def main():
    airport_code = os.getenv("AIRPORT_IATA", "HKT")
    start_date, end_date = get_default_dates()

    logger.info(f"Producer: получение данных {airport_code} {start_date} -> {end_date}")

    batches = extract_aerodatabox_fids_range_batch(
        airport_code=airport_code,
        code_type="IATA",
        start_date=start_date,
        end_date=end_date,
    )

    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )

    for batch in batches:
        producer.send("flight_events", value=batch)
        logger.info(f"Опубликован батч {batch["from_local"]} -> {batch["to_local"]}")

    producer.flush()
    producer.close()

    logger.info(f"Producer завершен: опубликовано {len(batches)} батчей")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()