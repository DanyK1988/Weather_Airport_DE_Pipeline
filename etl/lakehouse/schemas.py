from pyiceberg.partitioning import (
    PartitionField,
    PartitionSpec,
)
from pyiceberg.schema import Schema
from pyiceberg.transforms import (
    DayTransform,
    IdentityTransform,
)
from pyiceberg.types import (
    NestedField,
    StringType,
    TimestampType,
)

RAW_SCHEMA = Schema(
    NestedField(
        field_id=1,
        name="source",
        field_type=StringType(),
        required=True,
    ),
    NestedField(
        field_id=2,
        name="airport_code",
        field_type=StringType(),
        required=True,
    ),
    NestedField(
        field_id=3,
        name="code_type",
        field_type=StringType(),
        required=True,
    ),
    NestedField(
        field_id=4,
        name="from_local",
        field_type=StringType(),
        required=True,
    ),
    NestedField(
        field_id=5,
        name="to_local",
        field_type=StringType(),
        required=True,
    ),
    NestedField(
        field_id=6,
        name="direction",
        field_type=StringType(),
        required=True,
    ),
    NestedField(
        field_id=7,
        name="extracted_at",
        field_type=TimestampType(),
        required=True,
    ),
    NestedField(
        field_id=8,
        name="payload",
        field_type=StringType(),
        required=True,
    ),
)

RAW_PARTITICION_SPEC = PartitionSpec(
    PartitionField(
        source_id=2,
        field_id=1000,
        transform=IdentityTransform(),
        name="airport_code",
    ),
    PartitionField(
        source_id=7,
        field_id=1001,
        transform=DayTransform(),
        name="extracted_at_day",
    ),
)