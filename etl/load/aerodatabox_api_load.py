import json
import logging
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

def load_aerodatabox_raw(
        data: list,
        engine: Engine,
        schema: str = "raw",
        table_name: str = "aerodatabox_fids_range",
):
    # проверяем, есть ли данные для зугрузки
    if not data:
        logger.warning("Нет данных для загрузки")
        return

    # открываем транзакцию 
    with engine.begin() as conn:

        # создаем схему, если она еще не существует
        conn.execute(text(f"create schema if not exists {schema}"))

        # создаем таблицу, если она еще не существует
        conn.execute(text(f"""
            create table if not exists {schema}.{table_name} (
                source text,
                airport_code text,
                code_type text,
                from_local text,
                to_local text,
                direction text,
                extracted_at timestamptz,
                payload jsonb,
                unique (airport_code, code_type, from_local, to_local, direction)
                -- уникальный ключ защищает от повторной загрузки одного и того же батча
            )
            """))

        # подготовливаем список строк
        # каждая строка - это один батч
        rows = []
        for row in data:
            rows.append({
                # фиксируем источник данных
                "source": "aerodatabox",

                # мета-информация, полученная на этапе extract
                # используем .get(), чтобы не упасть при отсутствии ключа
                "airport_code": row.get("airport_code"),
                "code_type": row.get("code_type"),
                "from_local": row.get("from_local"),
                "to_local": row.get("to_local"),
                "direction": row.get("direction"),

                # payload сохраняем как JSON-строку
                "payload": json.dumps(row.get("payload"))
            })

        # SQL-запрос для вставки всех строк
        stmt = text(f"""
            insert into {schema}.{table_name}
            (source, airport_code, code_type, from_local, to_local, direction, extracted_at, payload)
            values (:source, :airport_code, :code_type, :from_local, :to_local, :direction, now(), :payload)

            -- если запись с таким ключом уже существует — пропускаем
            on conflict (airport_code, code_type, from_local, to_local, direction) do nothing
        """)

        # выполняем вставку 
        result = conn.execute(stmt, rows)

        # получаем количество реально вставленных строк
        inserted = result.rowcount if result.rowcount is not None else 0

    logger.info(f"Загружено {inserted} батчей в {schema}.{table_name}")