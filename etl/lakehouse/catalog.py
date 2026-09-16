import os

from pyiceberg.catalog import load_catalog

CATALOG_NAME = "flight_catalog"

NAMESPACE = "flights"
TABLE_NAME = "fids_range"


def get_iceberg_catalog():
    """
    Создаем подключение к REST Catalog Iceberg
    """

    catalog = load_catalog(
        CATALOG_NAME,
        **{
            "type": "rest",
            "uri": os.getenv(
                "ICEBERG_REST_URI",
                "http://iceberg-rest:8181",
            ),
            "warehouse": os.getenv(
                "ICEBERG_WAREHOUSE",
                "s3://flight-lakehouse/",
            ),
            "s3.endpoint": os.getenv(
                "MINIO_ENDPOINT",
                "http://minio:9000",
            ),
            "s3.path-style-access": "true",
            "s3.region": os.getenv(
                "AWS_REGION",
                "us-east-1",
            ),
            "s3.access-key-id": os.getenv(
                "MINIO_ROOT_USER",
            ),
            "s3.secret-access-key": os.getenv(
                "MINIO_ROOT_PASSWORD",
            ),

        },
    )

    return catalog