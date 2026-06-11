"""Shared constants, DDL, and helpers for all weather DAGs."""
from __future__ import annotations

import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "postgres_pipeline"
HTTP_CONN_ID     = "weather_conn"
API_ENDPOINT     = "data/3.0/onecall/timemachine"

DDL_RAW = """
CREATE TABLE IF NOT EXISTS pipeline_data.raw_weather (
    city         VARCHAR(100)             NOT NULL,
    logical_date DATE                     NOT NULL,
    raw_json     JSONB                    NOT NULL,
    fetched_at   TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (city, logical_date)
);
"""

DDL_TRANSFORMED = """
CREATE TABLE IF NOT EXISTS pipeline_data.transformed_weather (
    city            VARCHAR(100)             NOT NULL,
    logical_date    DATE                     NOT NULL,
    data_json       JSONB                    NOT NULL,
    quality_passed  BOOLEAN                  NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (city, logical_date)
);
"""

DDL_FINAL = """
CREATE TABLE IF NOT EXISTS pipeline_data.weather (
    id            SERIAL PRIMARY KEY,
    city          VARCHAR(100)             NOT NULL,
    lat           DOUBLE PRECISION         NOT NULL,
    lon           DOUBLE PRECISION         NOT NULL,
    timezone      VARCHAR(100),
    logical_date  DATE                     NOT NULL,
    temp_c        DOUBLE PRECISION,
    feels_like_c  DOUBLE PRECISION,
    humidity      INTEGER,
    pressure      INTEGER,
    uvi           DOUBLE PRECISION,
    wind_speed    DOUBLE PRECISION,
    clouds        INTEGER,
    description   VARCHAR(200),
    alert         BOOLEAN                  DEFAULT FALSE,
    fetched_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (city, logical_date)
);
"""

DDL_INDEX = """
CREATE UNIQUE INDEX IF NOT EXISTS uq_weather_city_day
    ON pipeline_data.weather (city, logical_date);
"""


def pg() -> PostgresHook:
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


def on_failure_callback(context: dict) -> None:
    ti  = context["task_instance"]
    exc = context.get("exception", "unknown error")
    logging.error(
        "Task failed | dag=%s | task=%s | run=%s | error=%s",
        ti.dag_id, ti.task_id, ti.run_id, exc,
    )


def ensure_schema() -> None:
    hook = pg()
    for ddl in (DDL_RAW, DDL_TRANSFORMED, DDL_FINAL, DDL_INDEX):
        hook.run(ddl)
