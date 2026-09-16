import json
from datetime import datetime, timezone

def batch_to_rows(batch: dict) -> list[dict]:
    payload = batch["payload"]

    if not isinstance(payload, str):
        payload = json.dumps(
            payload,
            ensure_ascii=False,
        )

    return [
        {
            "source": "aerodatabox",
            "airport_code": batch["airport_code"],
            "code_type": batch["code_type"],
            "from_local": batch["from_local"],
            "to_local": batch["to_local"],
            "direction": batch["direction"],
            "extracted_at": datetime.now(timezone.utc),
            "payload": payload,
        }
    ]

def batches_to_rows(batches: list[dict]) -> list[dict]:
    rows = []

    for batch in batches:
        rows.extend(batch_to_rows(batch))

    return rows