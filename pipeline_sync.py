"""Idempotent scheduled ingestion and merge for Fox ESS telemetry."""

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from typing import Any, Dict, List, Optional
from dateutil import parser as dt_parser
from dotenv import load_dotenv
import requests

load_dotenv()

BASE_URL = "https://www.foxesscloud.com"
HISTORY_ENDPOINT = "/op/v0/device/history/query"
DATA_FILE_PATH = Path("data/silver/telemetry_5min.json")


def build_fox_headers(path: str, api_key: str) -> Dict[str, str]:
    timestamp_ms = str(int(time.time() * 1000))
    signature_payload = f"{path}\r\n{api_key}\r\n{timestamp_ms}"
    signature_hash = hashlib.md5(signature_payload.encode("utf-8")).hexdigest()

    return {
        "token": api_key,
        "timestamp": timestamp_ms,
        "signature": signature_hash,
        "lang": "en",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (SolarDataPlatform/1.0)",
    }


def fetch_raw_history(
    api_key: str,
    device_sn: str,
    hours_back: int = 3,
    variables: Optional[List[str]] = None,
) -> Dict[str, Any]:
    if variables is None:
        variables = [
            "pvPower",
            "loadsPower",
            "gridConsumptionPower",
            "feedinPower",
            "batChargePower",
            "batDischargePower",
            "SoC",
            "batTemperature",
        ]

    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(hours=hours_back)

    payload = {
        "sn": device_sn,
        "variables": variables,
        "begin": int(start_utc.timestamp() * 1000),
        "end": int(now_utc.timestamp() * 1000),
    }

    headers = build_fox_headers(HISTORY_ENDPOINT, api_key)
    response = requests.post(
        f"{BASE_URL}{HISTORY_ENDPOINT}",
        headers=headers,
        json=payload,
        timeout=30,
    )
    response.raise_for_status()
    body = response.json()

    if body.get("errno") != 0:
        raise RuntimeError(
            f"Fox API Error {body.get('errno')}: {body.get('msg')}"
        )

    return body


def transform_history_to_silver(
    raw_response: Dict[str, Any],
) -> List[Dict[str, Any]]:
    results = raw_response.get("result", [])
    if not results or not isinstance(results, list):
        return []

    device_sn = results[0].get("deviceSN", "unknown_device")
    inverter_surrogate = (
        f"inv_{hashlib.sha256(device_sn.encode('utf-8')).hexdigest()[:12]}"
    )

    datas = results[0].get("datas", [])
    records_by_time: Dict[str, Dict[str, Any]] = {}

    mapping = {
        "pvPower": "pv_power_kw",
        "loadsPower": "loads_power_kw",
        "gridConsumptionPower": "grid_consumption_kw",
        "feedinPower": "grid_feed_in_kw",
        "batChargePower": "battery_charge_kw",
        "batDischargePower": "battery_discharge_kw",
        "SoC": "battery_soc_pct",
        "batTemperature": "battery_temp_celsius",
    }

    for var_block in datas:
        var_name = var_block.get("variable")
        time_points = var_block.get("data", [])

        for point in time_points:
            raw_time = point.get("time")
            raw_val = point.get("value")

            if not raw_time:
                continue

            if raw_time not in records_by_time:
                parsed_dt = dt_parser.parse(raw_time)
                utc_dt = parsed_dt.astimezone(timezone.utc)

                records_by_time[raw_time] = {
                    "reading_timestamp_utc": utc_dt.isoformat(),
                    "reading_timestamp_local": parsed_dt.isoformat(),
                    "inverter_id": inverter_surrogate,
                    "pv_power_kw": 0.0,
                    "loads_power_kw": 0.0,
                    "grid_consumption_kw": 0.0,
                    "grid_feed_in_kw": 0.0,
                    "battery_charge_kw": 0.0,
                    "battery_discharge_kw": 0.0,
                    "battery_soc_pct": 0.0,
                    "battery_temp_celsius": 0.0,
                }

            col_name = mapping.get(var_name)
            if col_name and raw_val is not None:
                records_by_time[raw_time][col_name] = round(float(raw_val), 3)

    return list(records_by_time.values())


def merge_and_persist(new_records: List[Dict[str, Any]]) -> int:
    """Merges incoming telemetry records idempotently with historical data."""
    existing_records: Dict[str, Dict[str, Any]] = {}

    if DATA_FILE_PATH.exists():
        try:
            with open(DATA_FILE_PATH, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                for item in loaded:
                    existing_records[item["reading_timestamp_utc"]] = item
        except Exception as exc:
            print(f"Warning: Could not read existing dataset: {exc}")

    for item in new_records:
        existing_records[item["reading_timestamp_utc"]] = item

    sorted_dataset = [
        existing_records[k] for k in sorted(existing_records.keys())
    ]

    DATA_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_FILE_PATH, "w", encoding="utf-8") as f:
        json.dump(sorted_dataset, f, indent=2)

    return len(sorted_dataset)


def main() -> None:
    api_key = os.getenv("FOX_API_KEY")
    device_sn = os.getenv("FOX_DEVICE_SN")

    if not api_key or not device_sn:
        print("Error: FOX_API_KEY or FOX_DEVICE_SN not set.")
        sys.exit(1)

    print("Fetching trailing 3 hours of telemetry...")
    raw = fetch_raw_history(api_key, device_sn, hours_back=3)
    silver_batch = transform_history_to_silver(raw)

    total_count = merge_and_persist(silver_batch)
    print(
        f"Merge complete. Dataset now contains {total_count} continuous 5-minute readings."
    )


if __name__ == "__main__":
    main()
