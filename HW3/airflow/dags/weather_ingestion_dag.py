"""
HW3 - weather_ingestion_dag (Kyiv)
Cross-DAG dependency - DAG 1 of 2.
Fetches weather data from OpenWeatherMap and stores raw JSON in raw_weather table.
"""
from __future__ import annotations

import json
import logging
from datetime import timedelta

import pendulum

from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.http.hooks.http import HttpHook
from airflow.sdk import dag, task, get_current_context

from common import (
    DDL_RAW,
    API_ENDPOINT,
    HTTP_CONN_ID,
    pg,
    on_failure_callback,
)

CITY = "Kyiv"


@dag(
    dag_id="weather_ingestion_dag",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    catchup=False,
    tags=["weather", "hw3", "kyiv", "ingestion"],
    render_template_as_native_obj=True,
    params={
        "lat":   Param(50.4501, type="number", description="Latitude of Kyiv"),
        "lon":   Param(30.5234, type="number", description="Longitude of Kyiv"),
        "units": Param(
            "metric",
            type="string",
            enum=["metric", "imperial", "standard"],
        ),
    },
    default_args={
        "retries":                   3,
        "retry_delay":               timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay":           timedelta(hours=1),
        "on_failure_callback":       on_failure_callback,
    },
)
def weather_ingestion_dag():
    @task()
    def ensure_raw_table() -> None:
        pg().run(DDL_RAW)
        logging.info("pipeline_data.raw_weather is ready.")

    @task(retries=3, retry_delay=timedelta(minutes=2), execution_timeout=timedelta(minutes=30))
    def fetch_and_store_raw(
        exec_date: str,
        lat: float = 0.0,
        lon: float = 0.0,
        units: str = "metric",
    ) -> str:
        """
        Fetch from OpenWeatherMap using Jinja-resolved params:
          {{ params.lat }}, {{ params.lon }}, {{ params.units }}, {{ ds }}
        Idempotent: skips API call if row already in raw_weather.
        Returns exec_date for XCom.
        """
        ctx  = get_current_context()
        hook = pg()

        if hook.get_first(
            "SELECT 1 FROM pipeline_data.raw_weather WHERE city=%s AND logical_date=%s",
            parameters=(CITY, exec_date),
        ):
            logging.info("raw_weather already present for %s %s - skipping.", CITY, exec_date)
            return exec_date

        api_key = Variable.get("WEATHER_API_KEY")
        http    = HttpHook(method="GET", http_conn_id=HTTP_CONN_ID)
        try:
            resp = http.run(
                endpoint=API_ENDPOINT,
                data={
                    "appid": api_key,
                    "lat":   lat,
                    "lon":   lon,
                    "dt":    int(ctx["data_interval_start"].timestamp()),
                    "units": units,
                },
            )
            raw = json.loads(resp.text)
        except Exception as exc:
            logging.error("API fetch failed for %s: %s", CITY, exc)
            raise

        if "data" not in raw or not raw["data"]:
            raise ValueError(
                f"Unexpected API response for {CITY} {exec_date}: {list(raw.keys())}"
            )

        hook.run(
            """
            INSERT INTO pipeline_data.raw_weather (city, logical_date, raw_json)
            VALUES (%s, %s, %s)
            ON CONFLICT (city, logical_date) DO UPDATE
                SET raw_json = EXCLUDED.raw_json, fetched_at = NOW()
            """,
            parameters=(CITY, exec_date, json.dumps(raw)),
        )
        logging.info("Stored raw_weather for %s %s.", CITY, exec_date)
        return exec_date

    ensure_raw_table() >> fetch_and_store_raw(
        exec_date="{{ ds }}",
        lat="{{ params.lat }}",
        lon="{{ params.lon }}",
        units="{{ params.units }}",
    )


weather_ingestion_dag()
