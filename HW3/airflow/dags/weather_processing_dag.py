"""
HW3 - weather_processing_dag (Kyiv)
Cross-DAG dependency - DAG 2 of 2.
Reads raw JSON from raw_weather (written by weather_ingestion_dag),
transforms it, runs data quality checks, branches on wind speed, and loads
into the final weather table.
"""
from __future__ import annotations

import json
import logging
from datetime import timedelta
from typing import Any

import pendulum

from airflow.models.param import Param
from airflow.sdk import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule

from common import (
    DDL_TRANSFORMED,
    DDL_FINAL,
    DDL_INDEX,
    pg,
    on_failure_callback,
)

CITY = "Kyiv"


@dag(
    dag_id="weather_processing_dag",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    catchup=False,
    tags=["weather", "hw3", "kyiv", "processing"],
    render_template_as_native_obj=True,
    params={
        "wind_threshold": Param(10.0, type="number",
                                description="Wind speed (m/s) threshold for alert"),
        "min_temp_c":     Param(-80.0, type="number"),
        "max_temp_c":     Param(60.0,  type="number"),
    },
    default_args={
        "retries":                   3,
        "retry_delay":               timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay":           timedelta(hours=1),
        "on_failure_callback":       on_failure_callback,
    },
)
def weather_processing_dag():
    # ── 0. Wait for ingestion DAG to finish for the same logical date ──────────
    wait_for_ingestion = ExternalTaskSensor(
        task_id="wait_for_ingestion",
        external_dag_id="weather_ingestion_dag",
        external_task_id="fetch_and_store_raw",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
        exponential_backoff=True,
        max_wait=timedelta(minutes=10),
    )

    # ── 1. Ensure tables ───────────────────────────────────────────────────────
    @task()
    def ensure_tables() -> None:
        hook = pg()
        hook.run(DDL_TRANSFORMED)
        hook.run(DDL_FINAL)
        hook.run(DDL_INDEX)
        logging.info("All processing tables ready.")

    # ── 2. TRANSFORM ──────────────────────────────────────────────────────────
    @task(execution_timeout=timedelta(minutes=30))
    def transform(exec_date: str) -> str:
        """Read raw_weather, produce clean record, store in transformed_weather.
        Idempotent: skips if row already present."""
        hook = pg()

        if hook.get_first(
            "SELECT 1 FROM pipeline_data.transformed_weather WHERE city=%s AND logical_date=%s",
            parameters=(CITY, exec_date),
        ):
            logging.info("transformed_weather already present for %s %s - skipping.", CITY, exec_date)
            return exec_date

        row = hook.get_first(
            "SELECT raw_json FROM pipeline_data.raw_weather WHERE city=%s AND logical_date=%s",
            parameters=(CITY, exec_date),
        )
        if not row:
            raise ValueError(f"No raw_weather row found for {CITY} {exec_date}")

        raw: dict     = row[0]
        current: dict = raw["data"][0]
        clean: dict[str, Any] = {
            "city":         CITY,
            "lat":          raw["lat"],
            "lon":          raw["lon"],
            "timezone":     raw["timezone"],
            "logical_date": exec_date,
            "temp_c":       current["temp"],
            "feels_like_c": current["feels_like"],
            "humidity":     current["humidity"],
            "pressure":     current["pressure"],
            "uvi":          current.get("uvi", 0.0),
            "wind_speed":   current["wind_speed"],
            "clouds":       current["clouds"],
            "description":  current["weather"][0]["description"],
        }

        hook.run(
            """
            INSERT INTO pipeline_data.transformed_weather
                (city, logical_date, data_json, quality_passed)
            VALUES (%s, %s, %s, FALSE)
            ON CONFLICT (city, logical_date) DO UPDATE
                SET data_json = EXCLUDED.data_json, created_at = NOW()
            """,
            parameters=(CITY, exec_date, json.dumps(clean)),
        )
        logging.info("Stored transformed_weather for %s %s.", CITY, exec_date)
        return exec_date

    # ── 3. DATA QUALITY CHECK ─────────────────────────────────────────────────
    @task(execution_timeout=timedelta(minutes=10))
    def quality_check(exec_date: str, min_temp: float = -80.0, max_temp: float = 60.0) -> str:
        """Validate transformed record; raises on failure, marks quality_passed=TRUE on success."""
        hook = pg()
        row  = hook.get_first(
            "SELECT data_json FROM pipeline_data.transformed_weather WHERE city=%s AND logical_date=%s",
            parameters=(CITY, exec_date),
        )
        if not row:
            raise ValueError(f"No transformed row for {CITY} {exec_date}")

        data   = row[0]
        errors = []

        if not (min_temp <= data["temp_c"] <= max_temp):
            errors.append(f"temp_c={data['temp_c']} outside [{min_temp}, {max_temp}]")
        if not (0 <= data["humidity"] <= 100):
            errors.append(f"humidity={data['humidity']} outside [0, 100]")
        if data["wind_speed"] < 0:
            errors.append(f"wind_speed={data['wind_speed']} is negative")
        if not (0 <= data.get("clouds", -1) <= 100):
            errors.append(f"clouds={data.get('clouds')} outside [0, 100]")
        if not (800 <= data.get("pressure", 0) <= 1100):
            errors.append(f"pressure={data.get('pressure')} outside [800, 1100] hPa")
        if not data.get("description"):
            errors.append("description is null or empty")

        if errors:
            raise ValueError(f"Quality FAILED for {CITY} {exec_date}: " + "; ".join(errors))

        hook.run(
            "UPDATE pipeline_data.transformed_weather SET quality_passed=TRUE "
            "WHERE city=%s AND logical_date=%s",
            parameters=(CITY, exec_date),
        )
        logging.info("Quality check PASSED for %s %s.", CITY, exec_date)
        return exec_date

    # ── 4. BRANCH – wind speed vs {{ params.wind_threshold }} ─────────────────
    @task.branch()
    def branch_wind(exec_date: str, threshold: float = 10.0) -> str:
        hook = pg()
        row  = hook.get_first(
            "SELECT data_json FROM pipeline_data.transformed_weather WHERE city=%s AND logical_date=%s",
            parameters=(CITY, exec_date),
        )
        if not row:
            raise ValueError(f"No transformed row for {CITY} {exec_date}")
        data = row[0]
        wind = data["wind_speed"]
        logging.info("Wind check %s: %.1f m/s (threshold=%.1f)", CITY, wind, threshold)
        if wind > threshold:
            return "alert_load"
        return "normal_load"

    # ── 5a. NORMAL LOAD ───────────────────────────────────────────────────────
    @task(execution_timeout=timedelta(minutes=30))
    def normal_load(exec_date: str) -> None:
        _upsert_final(exec_date, alert=False)

    # ── 5b. ALERT + LOAD ──────────────────────────────────────────────────────
    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
          execution_timeout=timedelta(minutes=30))
    def alert_load(exec_date: str) -> None:
        hook = pg()
        row  = hook.get_first(
            "SELECT data_json FROM pipeline_data.transformed_weather WHERE city=%s AND logical_date=%s",
            parameters=(CITY, exec_date),
        )
        if not row:
            raise ValueError(f"No transformed row for {CITY} {exec_date}")
        data = row[0]
        logging.warning(
            "HIGH WIND ALERT | city=%s | wind=%.1f m/s | date=%s",
            CITY, data["wind_speed"], exec_date,
        )
        _upsert_final(exec_date, alert=True)

    def _upsert_final(exec_date: str, *, alert: bool) -> None:
        hook = pg()
        existing = hook.get_first(
            "SELECT alert FROM pipeline_data.weather WHERE city=%s AND logical_date=%s",
            parameters=(CITY, exec_date),
        )
        if existing and bool(existing[0]) == alert:
            logging.info(
                "weather already loaded for %s %s (alert=%s) - skipping.",
                CITY, exec_date, alert,
            )
            return

        row = hook.get_first(
            "SELECT data_json FROM pipeline_data.transformed_weather WHERE city=%s AND logical_date=%s",
            parameters=(CITY, exec_date),
        )
        if not row:
            raise ValueError(f"No transformed row for {CITY} {exec_date}")
        d = row[0]
        hook.run(
            """
            INSERT INTO pipeline_data.weather
                (city, lat, lon, timezone, logical_date,
                 temp_c, feels_like_c, humidity, pressure,
                 uvi, wind_speed, clouds, description, alert)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (city, logical_date) DO UPDATE SET
                temp_c       = EXCLUDED.temp_c,
                feels_like_c = EXCLUDED.feels_like_c,
                humidity     = EXCLUDED.humidity,
                pressure     = EXCLUDED.pressure,
                uvi          = EXCLUDED.uvi,
                wind_speed   = EXCLUDED.wind_speed,
                clouds       = EXCLUDED.clouds,
                description  = EXCLUDED.description,
                alert        = EXCLUDED.alert,
                fetched_at   = NOW()
            """,
            parameters=(
                d["city"], d["lat"], d["lon"], d["timezone"], d["logical_date"],
                d["temp_c"], d["feels_like_c"], d["humidity"], d["pressure"],
                d["uvi"], d["wind_speed"], d["clouds"], d["description"],
                alert,
            ),
        )
        logging.info("Loaded weather for %s %s (alert=%s).", CITY, exec_date, alert)

    # ── Wire up ────────────────────────────────────────────────────────────────
    ensure_tables_task = ensure_tables()
    raw_ld             = transform(exec_date="{{ ds }}")
    quality_ld         = quality_check(
        raw_ld,
        min_temp="{{ params.min_temp_c }}",
        max_temp="{{ params.max_temp_c }}",
    )
    wind_decision      = branch_wind(quality_ld, threshold="{{ params.wind_threshold }}")
    load_normal        = normal_load(quality_ld)
    load_alert         = alert_load(quality_ld)

    wait_for_ingestion >> ensure_tables_task >> raw_ld
    wind_decision >> [load_normal, load_alert]


weather_processing_dag()
