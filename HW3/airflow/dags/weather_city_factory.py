"""
HW3 – Weather City DAG Factory
================================
Design-pattern requirements:
  • Factory that generates one full DAG per city
  • Explicit external storage after every step (raw_weather → transformed_weather → weather)
  • Data-quality gate between transform and load
  • Idempotent steps → resume from the last failed step without re-running succeeded ones
  • Jinja / DAG params for every previously hard-coded value
  • Retry logic + on-failure callback

Cities handled here: Lviv, Odesa, Kharkiv, Ivano-Frankivsk.
Kyiv is handled by the cross-DAG pair  weather_ingestion_dag / weather_processing_dag.
"""
from __future__ import annotations

import json
import logging
from datetime import timedelta
from typing import Any

import pendulum

from airflow.models.param import Param
from airflow.models import Variable
from airflow.providers.http.hooks.http import HttpHook
from airflow.sdk import dag, task, get_current_context
from airflow.utils.trigger_rule import TriggerRule

from common import (
    API_ENDPOINT,
    HTTP_CONN_ID,
    pg,
    on_failure_callback,
    ensure_schema,
)

# ---------------------------------------------------------------------------
# City catalogue  (Kyiv is in its own ingestion/processing DAG pair)
# ---------------------------------------------------------------------------
CITIES: dict[str, dict[str, float]] = {
    "Lviv":            {"lat": 49.8397, "lon": 24.0297},
    "Odesa":           {"lat": 46.4825, "lon": 30.7233},
    "Kharkiv":         {"lat": 49.9935, "lon": 36.2304},
    "Ivano-Frankivsk": {"lat": 48.9226, "lon": 24.7111},
}


# ---------------------------------------------------------------------------
# DAG factory
# ---------------------------------------------------------------------------

def create_weather_dag(city_name: str, lat: float, lon: float):
    """Return a fully-wired Airflow DAG for *city_name*."""

    dag_id = "weather_" + city_name.lower().replace(" ", "_").replace("-", "_")

    @dag(
        dag_id=dag_id,
        schedule="@daily",
        start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
        catchup=True,
        tags=["weather", "hw3", "factory"],
        params={
            "wind_threshold": Param(
                10.0,
                type="number",
                description="Wind speed (m/s) that triggers an alert",
            ),
            "units": Param(
                "metric",
                type="string",
                enum=["metric", "imperial", "standard"],
                description="Unit system passed to OpenWeatherMap",
            ),
            "min_temp_c": Param(-80.0, type="number",
                                description="Quality check: minimum allowed temperature (°C)"),
            "max_temp_c": Param(60.0,  type="number",
                                description="Quality check: maximum allowed temperature (°C)"),
        },
        default_args={
            "retries":               3,
            "retry_delay":           timedelta(minutes=5),
            "retry_exponential_backoff": True,
            "max_retry_delay":       timedelta(hours=1),
            "on_failure_callback":   on_failure_callback,
            "sla":                   timedelta(hours=2),
        },
        render_template_as_native_obj=True,
    )
    def _weather_dag():

        # ── 0. Bootstrap tables ───────────────────────────────────────────────
        @task()
        def ensure_tables() -> None:
            """Idempotent DDL: create raw / transformed / final tables."""
            ensure_schema()
            logging.info("All pipeline_data tables are ready.")

        # ── 1. EXTRACT – fetch from API, persist to raw_weather ───────────────
        @task(retries=3, retry_delay=timedelta(minutes=2),
              execution_timeout=timedelta(minutes=30))
        def extract(logical_date: str, units: str = '') -> str:
            """
            Pull timemachine data from OpenWeatherMap.
            If a row for (city, logical_date) already exists in raw_weather
            we skip the API call → idempotent resume.
            Returns: logical_date string (used as XCom to downstream tasks).
            """
            ctx  = get_current_context()
            hook = pg()

            if hook.get_first(
                "SELECT 1 FROM pipeline_data.raw_weather "
                "WHERE city = %s AND logical_date = %s",
                parameters=(city_name, logical_date),
            ):
                logging.info("raw_weather row already exists for %s %s – skipping fetch.",
                             city_name, logical_date)
                return logical_date

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
                logging.error("Fetch failed for %s: %s", city_name, exc)
                raise

            if "data" not in raw or not raw["data"]:
                raise ValueError(
                    f"Unexpected API response for {city_name} {logical_date}: {list(raw.keys())}"
                )

            hook.run(
                """
                INSERT INTO pipeline_data.raw_weather (city, logical_date, raw_json)
                VALUES (%s, %s, %s)
                ON CONFLICT (city, logical_date) DO UPDATE
                    SET raw_json = EXCLUDED.raw_json, fetched_at = NOW()
                """,
                parameters=(city_name, logical_date, json.dumps(raw)),
            )
            logging.info("Stored raw_weather for %s %s.", city_name, logical_date)
            return logical_date

        # ── 2. TRANSFORM – read raw, clean, persist to transformed_weather ────
        @task(execution_timeout=timedelta(minutes=30))
        def transform(logical_date: str) -> str:
            """
            Read from raw_weather, produce a clean dict, store in
            transformed_weather.  Idempotent – skips if row already present.
            Returns: logical_date (pass-through XCom).
            """
            hook = pg()

            if hook.get_first(
                "SELECT 1 FROM pipeline_data.transformed_weather "
                "WHERE city = %s AND logical_date = %s",
                parameters=(city_name, logical_date),
            ):
                logging.info(
                    "transformed_weather row already exists for %s %s – skipping.",
                    city_name, logical_date,
                )
                return logical_date

            row = hook.get_first(
                "SELECT raw_json FROM pipeline_data.raw_weather "
                "WHERE city = %s AND logical_date = %s",
                parameters=(city_name, logical_date),
            )
            if not row:
                raise ValueError(f"No raw_weather row found for {city_name} {logical_date}")

            raw     = row[0]
            current = raw["data"][0]
            clean: dict[str, Any] = {
                "city":         city_name,
                "lat":          raw["lat"],
                "lon":          raw["lon"],
                "timezone":     raw["timezone"],
                "logical_date": logical_date,
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
                parameters=(city_name, logical_date, json.dumps(clean)),
            )
            logging.info("Stored transformed_weather for %s %s.", city_name, logical_date)
            return logical_date

        # ── 3. DATA QUALITY CHECK ─────────────────────────────────────────────
        @task(execution_timeout=timedelta(minutes=10))
        def quality_check(logical_date: str, min_temp: float = 0.0, max_temp: float = 0.0) -> str:
            """
            Validate transformed record against DAG params thresholds.
            Marks quality_passed = TRUE on success; raises on failure.
            Returns: logical_date.
            """
            hook = pg()
            row  = hook.get_first(
                "SELECT data_json FROM pipeline_data.transformed_weather "
                "WHERE city = %s AND logical_date = %s",
                parameters=(city_name, logical_date),
            )
            if not row:
                raise ValueError(f"No transformed row for {city_name} {logical_date}")

            data    = row[0]
            errors: list[str] = []

            if not (min_temp <= data["temp_c"] <= max_temp):
                errors.append(
                    f"temp_c={data['temp_c']} outside [{min_temp}, {max_temp}]"
                )
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
                raise ValueError(
                    f"Data quality FAILED for {city_name} {logical_date}: "
                    + "; ".join(errors)
                )

            hook.run(
                "UPDATE pipeline_data.transformed_weather "
                "SET quality_passed = TRUE "
                "WHERE city = %s AND logical_date = %s",
                parameters=(city_name, logical_date),
            )
            logging.info("Quality check PASSED for %s %s.", city_name, logical_date)
            return logical_date

        # ── 4. BRANCH – wind speed check ──────────────────────────────────────
        @task.branch()
        def branch_wind(logical_date: str, threshold: float = 10.0) -> str:
            """
            Read wind_speed from transformed_weather; route to alert_load
            when it exceeds {{ params.wind_threshold }}, else normal_load.
            """
            hook = pg()
            row  = hook.get_first(
                "SELECT data_json FROM pipeline_data.transformed_weather "
                "WHERE city = %s AND logical_date = %s",
                parameters=(city_name, logical_date),
            )
            if not row:
                raise ValueError(f"No transformed row for {city_name} {logical_date}")
            data  = row[0]
            wind  = data["wind_speed"]
            logging.info(
                "Wind check for %s: %.1f m/s (threshold=%.1f from params)",
                city_name, wind, threshold,
            )
            if wind > threshold:
                return "alert_load"
            return "normal_load"

        # ── 5a. NORMAL LOAD ───────────────────────────────────────────────────
        @task(execution_timeout=timedelta(minutes=30))
        def normal_load(logical_date: str) -> None:
            """Read from transformed_weather and upsert into weather (no alert)."""
            _load_final(logical_date, alert=False)

        # ── 5b. ALERT + LOAD ──────────────────────────────────────────────────
        @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
              execution_timeout=timedelta(minutes=30))
        def alert_load(logical_date: str) -> None:
            """Log high-wind alert, then upsert into weather with alert=TRUE."""
            hook = pg()
            row  = hook.get_first(
                "SELECT data_json FROM pipeline_data.transformed_weather "
                "WHERE city = %s AND logical_date = %s",
                parameters=(city_name, logical_date),
            )
            if not row:
                raise ValueError(f"No transformed row for {city_name} {logical_date}")
            data = row[0]
            logging.warning(
                "HIGH WIND ALERT | city=%s | wind=%.1f m/s | date=%s",
                city_name, data["wind_speed"], logical_date,
            )
            _load_final(logical_date, alert=True)

        def _load_final(logical_date: str, *, alert: bool) -> None:
            hook     = pg()
            existing = hook.get_first(
                "SELECT alert FROM pipeline_data.weather WHERE city = %s AND logical_date = %s",
                parameters=(city_name, logical_date),
            )
            if existing and bool(existing[0]) == alert:
                logging.info(
                    "weather row already loaded for %s %s (alert=%s) - skipping.",
                    city_name, logical_date, alert,
                )
                return

            row = hook.get_first(
                "SELECT data_json FROM pipeline_data.transformed_weather "
                "WHERE city = %s AND logical_date = %s",
                parameters=(city_name, logical_date),
            )
            if not row:
                raise ValueError(f"No transformed row for {city_name} {logical_date}")
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
            logging.info("Loaded weather row for %s %s (alert=%s).",
                         city_name, logical_date, alert)

        # ── Wire up ───────────────────────────────────────────────────────────
        ensure_tables_task = ensure_tables()
        raw_ld             = extract(logical_date="{{ ds }}", units="{{ params.units }}")
        transformed_ld     = transform(raw_ld)
        quality_ld         = quality_check(
            transformed_ld,
            min_temp="{{ params.min_temp_c }}",
            max_temp="{{ params.max_temp_c }}",
        )
        wind_decision      = branch_wind(quality_ld, threshold="{{ params.wind_threshold }}")
        load_normal        = normal_load(quality_ld)
        load_alert         = alert_load(quality_ld)

        ensure_tables_task >> raw_ld
        wind_decision >> [load_normal, load_alert]

    return _weather_dag()


# ---------------------------------------------------------------------------
# Instantiate one DAG per city  (factory call)
# ---------------------------------------------------------------------------
for _city, _coords in CITIES.items():
    globals()[
        "weather_" + _city.lower().replace(" ", "_").replace("-", "_")
    ] = create_weather_dag(_city, _coords["lat"], _coords["lon"])
