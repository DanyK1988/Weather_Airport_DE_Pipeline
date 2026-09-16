import logging
import os

import pyarrow as pa

from etl.landing.minio_client import (
    get_minio_client,
    list_raw_batches,
    read_raw_batch,
)
from etl.lakehouse.catalog import (
    get_iceberg_catalog,
    NAMESPACE,
    TABLE_NAME,
)
from etl.lakehouse.schemas import (
    RAW_SCHEMA,
    RAW_PARTITICION_SPEC,
)
from etl.lakehouse.transforms import batches_to_rows

logger = logging.getLogger(__name__)


def main():
    minio_client = get_minio_client()

    prefix = os.getenv(
        "MINIO_PREFIX",
        "",
    )

    object_names = list_raw_batches(
        minio_client,
        prefix=prefix,
    )

    if not object_names:
        logger.error(
            f"Нет объектов в MinIO prefix={prefix}"
        )
        return

    batches = []

    for object_name in object_names:
        batch = read_raw_batch(
            minio_client,
            object_name,
        )

        if batch:
            batches.append(batch)

    rows = batches_to_rows(batches)

    if not rows:
        logger.error(
            "Нет данных для загрузки"
        )
        return

    catalog = get_iceberg_catalog()

    catalog.create_namespace_if_not_exists(
        NAMESPACE
    )

    table = catalog.create_table_if_not_exists(
        identifier=f"{NAMESPACE}.{TABLE_NAME}",
        schema=RAW_SCHEMA,
        partition_spec=RAW_PARTITICION_SPEC,
    )

    table_schema = table.schema().as_arrow()

    table_data = pa.Table.from_pylist(
        rows,
        schema=table_schema,
    )

    table.append(table_data)

    logger.info(
        f"Добавлено строк: {len(rows)}"
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()