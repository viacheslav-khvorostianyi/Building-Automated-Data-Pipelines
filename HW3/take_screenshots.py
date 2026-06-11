"""
Playwright script: logs into Airflow UI + Flower, takes screenshots
demonstrating the HW3 weather pipeline workflow.

Run from HW3/ directory:
    python3 take_screenshots.py
"""
from __future__ import annotations

import time
from pathlib import Path
from playwright.sync_api import sync_playwright, Page

BASE    = "http://localhost:8080"
FLOWER  = "http://localhost:5555"
IMG_DIR = Path(__file__).parent / "img"
IMG_DIR.mkdir(exist_ok=True)


def login(page: Page) -> None:
    page.goto(f"{BASE}/login")
    page.wait_for_load_state("networkidle")
    page.fill("input[name=username], #username", "airflow")
    page.fill("input[name=password], #password", "airflow")
    page.locator("button[type=submit], input[type=submit]").first.click()
    page.wait_for_load_state("networkidle")
    time.sleep(1)


def screenshot(page: Page, path: str, msg: str = "") -> None:
    page.wait_for_load_state("networkidle")
    time.sleep(2)
    full = IMG_DIR / path
    page.screenshot(path=str(full), full_page=True)
    print(f"  saved {full}" + (f"  ({msg})" if msg else ""))


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        page    = browser.new_page(viewport={"width": 1600, "height": 900})

        print("Logging in...")
        login(page)

        # ── DAG list ──────────────────────────────────────────────────────────
        print("DAG list...")
        page.goto(f"{BASE}/dags")
        screenshot(page, "dag_list.png", "all 6 DAGs")

        # ── Per-DAG grid views ────────────────────────────────────────────────
        dag_shots = [
            ("weather_ingestion_dag",  "ingestion_dag_grid.png",  "Kyiv ingestion"),
            ("weather_processing_dag", "processing_dag_grid.png", "Kyiv processing + ExternalTaskSensor"),
            ("weather_lviv",           "lviv_dag_grid.png",        "factory DAG – Lviv"),
            ("weather_odesa",          "odesa_dag_grid.png",       "factory DAG – Odesa"),
            ("weather_kharkiv",        "kharkiv_dag_grid.png",     "factory DAG – Kharkiv"),
            ("weather_ivano_frankivsk","ivano_dag_grid.png",       "factory DAG – Ivano-Frankivsk"),
        ]

        for dag_id, filename, msg in dag_shots:
            print(f"{dag_id}...")
            page.goto(f"{BASE}/dags/{dag_id}")
            screenshot(page, filename, msg)

        # ── Flower ────────────────────────────────────────────────────────────
        print("Flower workers...")
        page.goto(FLOWER)
        screenshot(page, "flower_workers_new.png", "Celery workers")

        print("Flower tasks...")
        page.goto(f"{FLOWER}/tasks")
        screenshot(page, "flower_tasks_new.png", "Celery task history")

        browser.close()
        print(f"\nDone — screenshots in {IMG_DIR}/")


if __name__ == "__main__":
    main()
