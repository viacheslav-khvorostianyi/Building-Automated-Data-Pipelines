from __future__ import annotations

import json
import logging
from datetime import timedelta

import pendulum

from airflow.models import Variable
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task, get_current_context
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# ── City catalogue ────────────────────────────────────────────────────────────
CITIES: dict[str, dict[str, float]] = {
    "Kyiv":            {"lat": 50.4501, "lon": 30.5234},
    "Lviv":            {"lat": 49.8397, "lon": 24.0297},
    "Odesa":           {"lat": 46.4825, "lon": 30.7233},
    "Kharkiv":         {"lat": 49.9935, "lon": 36.2304},
    "Ivano-Frankivsk": {"lat": 48.9226, "lon": 24.7111},
}

# Alert fires when wind speed exceeds this value (m/s)
WIND_SPEED_THRESHOLD: float = 10.0

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
    alert         BOOLEAN                  DEFAULT FALSE,
    fetched_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
"""

CREATE_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS uq_weather_city_day
    ON pipeline_data.weather (city, logical_date);
"""

INSERT_SQL = """
INSERT INTO pipeline_data.weather
    (city, lat, lon, timezone, logical_date,
     temp_c, feels_like_c, humidity, pressure,
     uvi, wind_speed, clouds, description, alert)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (city, logical_date) DO UPDATE SET
    temp_c        = EXCLUDED.temp_c,
    feels_like_c  = EXCLUDED.feels_like_c,
    humidity      = EXCLUDED.humidity,
    pressure      = EXCLUDED.pressure,
    uvi           = EXCLUDED.uvi,
    wind_speed    = EXCLUDED.wind_speed,
    clouds        = EXCLUDED.clouds,
    description   = EXCLUDED.description,
    alert         = EXCLUDED.alert,
    fetched_at    = NOW();
"""

# ── Helpers ───────────────────────────────────────────────────────────────────

def _city_group_id(city: str) -> str:
    """Turn a city name into a valid Airflow task-group ID."""
    return city.lower().replace(" ", "_").replace("-", "_")


def _on_failure(context: dict) -> None:
    """Global failure callback — logs a structured error summary."""
    ti  = context["task_instance"]
    exc = context.get("exception", "unknown error")
    logging.error(
        "❌ Task failed | dag=%s | task=%s | run=%s | error=%s",
        ti.dag_id, ti.task_id, ti.run_id, exc,
    )


def _upsert(data: dict, *, alert: bool) -> None:
    """Insert or update one weather row (shared by both load tasks)."""
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
            alert,
        ),
    )
    logging.info(
        "Upserted %s for %s (alert=%s, wind=%.1f m/s)",
        data["city"], data["logical_date"], alert, data["wind_speed"],
    )


# ── DAG ───────────────────────────────────────────────────────────────────────
@dag(
    dag_id="example_weather_dag",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 3, 20, tz="UTC"),
    catchup=True,
    tags=["weather", "hw2"],
    default_args={
        # ── Retry logic (HW2 requirement) ────────────────────────────────────
        "retries":                    3,
        "retry_delay":                timedelta(minutes=5),
        "retry_exponential_backoff":  True,   # 5 min → 10 min → 20 min
        "max_retry_delay":            timedelta(hours=1),
        "on_failure_callback":        _on_failure,
    },
)
def example_weather_dag():

    # ── 0. Create / migrate table (runs once before any city group) ───────────
    @task()
    def create_table() -> None:
        """Ensure pipeline_data.weather exists (idempotent)."""
        hook = PostgresHook(postgres_conn_id="postgres_pipeline")
        hook.run(CREATE_TABLE_SQL)
        hook.run(CREATE_INDEX_SQL)
        logging.info("Table pipeline_data.weather is ready.")

    # ── Per-city task factory functions ───────────────────────────────────────
    # Defined inside the @dag function so they inherit default_args, but
    # *outside* the for-loop so each @task creates fresh operator instances
    # per TaskGroup call without closure-variable pitfalls.

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def fetch(lat: float, lon: float) -> dict:
        """Call OpenWeatherMap timemachine API; push raw JSON via XCom."""
        ctx     = get_current_context()
        api_key = Variable.get("WEATHER_API_KEY")
        hook    = HttpHook(method="GET", http_conn_id="weather_conn")
        try:
            resp = hook.run(
                endpoint="data/3.0/onecall/timemachine",
                data={
                    "appid": api_key,
                    "lat":   lat,
                    "lon":   lon,
                    "dt":    int(ctx["data_interval_start"].timestamp()),
                    "units": "metric",
                },
            )
            return json.loads(resp.text)
        except Exception as exc:
            logging.error("Fetch failed for lat=%s lon=%s: %s", lat, lon, exc)
            raise   # re-raise so Airflow can retry

    @task()
    def transform(raw: dict, city: str) -> dict:
        """
        Parse timemachine response and push a clean record via XCom.

        timemachine wraps the hourly slot in data["data"][0]
        (unlike onecall which uses data["current"]).
        """
        ctx     = get_current_context()
        current = raw["data"][0]
        return {
            "city":         city,
            "lat":          raw["lat"],
            "lon":          raw["lon"],
            "timezone":     raw["timezone"],
            "logical_date": ctx["ds"],          # e.g. "2026-03-20"
            "temp_c":       current["temp"],
            "feels_like_c": current["feels_like"],
            "humidity":     current["humidity"],
            "pressure":     current["pressure"],
            "uvi":          current.get("uvi", 0.0),
            "wind_speed":   current["wind_speed"],
            "clouds":       current["clouds"],
            "description":  current["weather"][0]["description"],
        }

    @task.branch()
    def branch_wind(data: dict, group_id: str) -> str:
        """
        BranchOperator: route to alert_load when wind speed > threshold,
        otherwise to normal_load.

        Returns the *full* task_id (including the TaskGroup prefix) so
        Airflow knows which downstream task to run.
        """
        wind = data["wind_speed"]
        logging.info(
            "Wind check for %s: %.1f m/s (threshold %.1f m/s)",
            data["city"], wind, WIND_SPEED_THRESHOLD,
        )
        if wind > WIND_SPEED_THRESHOLD:
            return f"{group_id}.alert_load"
        return f"{group_id}.normal_load"

    @task()
    def normal_load(data: dict) -> None:
        """Standard path: upsert without alert flag."""
        _upsert(data, alert=False)

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def alert_load(data: dict) -> None:
        """Alert path: log a warning then upsert with alert=True."""
        logging.warning(
            "⚠ HIGH WIND ALERT | city=%s | wind=%.1f m/s | date=%s",
            data["city"], data["wind_speed"], data["logical_date"],
        )
        _upsert(data, alert=True)

    # ── Wire up pipeline ──────────────────────────────────────────────────────
    table_ready = create_table()

    for city_name, coords in CITIES.items():
        group_id = _city_group_id(city_name)

        with TaskGroup(group_id=group_id) as city_group:
            # 1 ▶ FETCH  —  raw API response pushed to XCom
            raw_data = fetch(coords["lat"], coords["lon"])

            # 2 ▶ TRANSFORM  —  clean record pushed to XCom
            clean_data = transform(raw_data, city_name)

            # 3 ▶ BRANCH  —  wind-speed check routes to one of two load tasks
            decision = branch_wind(clean_data, group_id)

            # 4a ▶ NORMAL LOAD  (no alert)
            nl = normal_load(clean_data)

            # 4b ▶ ALERT + LOAD  (high wind)
            al = alert_load(clean_data)

            # branch_wind decides which of the two load tasks runs
            decision >> [nl, al]

        # Table must exist before any city group starts
        table_ready >> city_group


example_weather_dag()
