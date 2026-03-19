"""
example_dag.py — Weather ETL DAG for Airflow 3.0 / CeleryExecutor
"""

from __future__ import annotations

import json
import logging

import pendulum

from airflow.models import Variable
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, get_current_context, task

# ── City coordinates ──────────────────────────────────────────────────────────
CITIES: dict[str, dict[str, float]] = {
    "Kyiv":            {"lat": 50.4501, "lon": 30.5234},
    "Lviv":            {"lat": 49.8397, "lon": 24.0297},
    "Odesa":           {"lat": 46.4825, "lon": 30.7233},
    "Kharkiv":         {"lat": 49.9935, "lon": 36.2304},
    "Ivano-Frankivsk": {"lat": 48.9226, "lon": 24.7111},
}

_COORD_TO_CITY: dict[tuple[float, float], str] = {
    (v["lat"], v["lon"]): k for k, v in CITIES.items()
}

# ── SQL ───────────────────────────────────────────────────────────────────────
CREATE_TABLE_SQL = """
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
    fetched_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
"""

# One row per city per scheduled date — re-running the same DAG run is safe
CREATE_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS uq_weather_city_day
    ON pipeline_data.weather (city, logical_date);
"""

INSERT_SQL = """
INSERT INTO pipeline_data.weather
    (city, lat, lon, timezone, logical_date,
     temp_c, feels_like_c, humidity, pressure,
     uvi, wind_speed, clouds, description)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (city, logical_date) DO UPDATE SET
    temp_c        = EXCLUDED.temp_c,
    feels_like_c  = EXCLUDED.feels_like_c,
    humidity      = EXCLUDED.humidity,
    pressure      = EXCLUDED.pressure,
    uvi           = EXCLUDED.uvi,
    wind_speed    = EXCLUDED.wind_speed,
    clouds        = EXCLUDED.clouds,
    description   = EXCLUDED.description,
    fetched_at    = NOW();
"""


@dag(
    dag_id="example_weather_dag",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 3, 20, tz="UTC"),
    catchup=True,
    tags=["weather"],
)
def example_weather_dag():

    # ── Create / migrate table ────────────────────────────────────────────────
    @task()
    def create_table() -> None:
        """Create table and unique index (idempotent)."""
        hook = PostgresHook(postgres_conn_id="postgres_pipeline")
        hook.run(CREATE_TABLE_SQL)   # no-op if table already exists
        hook.run(CREATE_INDEX_SQL)   # no-op if index already exists
        logging.info("Table pipeline_data.weather is ready.")

    # ── Extract ───────────────────────────────────────────────────────────────
    # /timemachine returns weather for a specific Unix timestamp (dt).
    # dt is a Jinja template → resolved at task-execution time to the
    # Unix timestamp of data_interval_start (the logical date being processed).
    # A catchup run for 2026-03-20 therefore fetches 2026-03-20 data.
    extract = HttpOperator.partial(
        task_id="extract",
        http_conn_id="weather_conn",
        endpoint="data/3.0/onecall/timemachine",
        method="GET",
        response_filter=lambda x: json.loads(x.text),
        log_response=True,
    ).expand(
        data=[
            {
                "appid": Variable.get("WEATHER_API_KEY"),
                "lat":   coords["lat"],
                "lon":   coords["lon"],
                "dt":    "{{ logical_date.int_timestamp }}",
                "units": "metric",
            }
            for coords in CITIES.values()
        ]
    )

    # ── Transform ─────────────────────────────────────────────────────────────
    @task()
    def transform(data: dict) -> dict:
        """Parse a timemachine response.

        ⚠ Timemachine response differs from onecall:
            onecall     → data["current"]
            timemachine → data["data"][0]
        """
        ctx     = get_current_context()
        current = data["data"][0]        # ← timemachine wraps fields in a list
        return {
            "city":         _COORD_TO_CITY.get((data["lat"], data["lon"]), data["timezone"]),
            "lat":          data["lat"],
            "lon":          data["lon"],
            "timezone":     data["timezone"],
            "logical_date": ctx["ds"],   # "2026-03-20" — the date being processed
            "temp_c":       current["temp"],
            "feels_like_c": current["feels_like"],
            "humidity":     current["humidity"],
            "pressure":     current["pressure"],
            "uvi":          current.get("uvi", 0.0),
            "wind_speed":   current["wind_speed"],
            "clouds":       current["clouds"],
            "description":  current["weather"][0]["description"],
        }

    # ── Load ──────────────────────────────────────────────────────────────────
    @task()
    def load(data: dict) -> None:
        """Upsert one weather record into pipeline_data.weather."""
        hook = PostgresHook(postgres_conn_id="postgres_pipeline")
        hook.run(
            INSERT_SQL,
            parameters=(
                data["city"],
                data["lat"],
                data["lon"],
                data["timezone"],
                data["logical_date"],
                data["temp_c"],
                data["feels_like_c"],
                data["humidity"],
                data["pressure"],
                data["uvi"],
                data["wind_speed"],
                data["clouds"],
                data["description"],
            ),
        )
        logging.info("Upserted %s for %s", data["city"], data["logical_date"])

    # ── Pipeline ──────────────────────────────────────────────────────────────
    table_ready = create_table()
    processed   = transform.expand(data=extract.output)   # 5 × transform
    load_tasks  = load.expand(data=processed)              # 5 × load

    table_ready >> load_tasks   # table must exist before any write


example_weather_dag()
