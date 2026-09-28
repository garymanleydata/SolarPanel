"""Automated Fox ESS telemetry ingestion, cumulative meter tracking, and Octopus Go tariff enrichment."""

import argparse
from datetime import datetime, time, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import time as time_mod
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo
from dateutil import parser as dt_parser
from dotenv import load_dotenv
import requests

# Import C&J Data Quality & Verification Module
import data_quality

# 1. Deterministic Path Resolution
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data" / "silver"
OUTPUT_FILE_PATH = DATA_DIR / "telemetry_5min.json"
UK_TZ = ZoneInfo("Europe/London")

# Load .env explicitly from the script root
load_dotenv(PROJECT_ROOT / ".env")

BASE_URL = "https://www.foxesscloud.com"
HISTORY_ENDPOINT = "/op/v0/device/history/query"

# Target variables: instantaneous power metrics and cumulative registers
TARGET_VARIABLES = [
    # Instantaneous Power Metrics (kW / % / °C)
    "pvPower",
    "loadsPower",
    "gridConsumptionPower",
    "feedinPower",
    "batChargePower",
    "batDischargePower",
    "SoC",
    "batTemperature",
    # Cumulative Meter Registers (kWh)
    "generation",
    "gridConsumption",
    "feedin",
    "loads",
    "chargeEnergyToTal",
    "dischargeEnergyToTal",
]

# Variable mapping: API camelCase -> standardised snake_case columns
VARIABLE_MAPPING = {
    "pvPower": "pv_power_kw",
    "loadsPower": "loads_power_kw",
    "gridConsumptionPower": "grid_consumption_kw",
    "feedinPower": "grid_feed_in_kw",
    "batChargePower": "battery_charge_kw",
    "batDischargePower": "battery_discharge_kw",
    "SoC": "battery_soc_pct",
    "batTemperature": "battery_temp_celsius",
    "generation": "cum_generation_kwh",
    "gridConsumption": "cum_grid_import_kwh",
    "feedin": "cum_grid_export_kwh",
    "loads": "cum_loads_kwh",
    "chargeEnergyToTal": "cum_battery_charge_kwh",
    "dischargeEnergyToTal": "cum_battery_discharge_kwh",
}

# Octopus Go Tariff Schedule (Region J - South East England)
OCTOPUS_GO_CONFIG = {
    "tariff_name": "Octopus Go",
    "region": "J",
    "off_peak_rate_p_kwh": 9.00,
    "peak_rate_p_kwh": 25.50,
    "export_rate_p_kwh": 15.00,  # Outgoing Octopus Fixed
    "off_peak_start": time(0, 30),
    "off_peak_end": time(5, 30),  # Corrected off-peak window to 05:30
}


def build_fox_headers(path: str, api_key: str) -> Dict[str, str]:
    """Generates the MD5 signature and request headers for Fox ESS."""
    timestamp_ms = str(int(time_mod.time() * 1000))
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


def fetch_history_slice(
    api_key: str,
    device_sn: str,
    start_dt: datetime,
    end_dt: datetime,
) -> Dict[str, Any]:
    """Queries a single slice (<= 24 hours) from the Fox ESS history endpoint."""
    payload = {
        "sn": device_sn,
        "variables": TARGET_VARIABLES,
        "begin": int(start_dt.timestamp() * 1000),
        "end": int(end_dt.timestamp() * 1000),
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


def fetch_historical_window(
    api_key: str,
    device_sn: str,
    total_hours: float,
) -> List[Dict[str, Any]]:
    """Chunks queries into 24-hour windows to bypass the Fox ESS API constraint."""
    now_utc = datetime.now(timezone.utc)
    earliest_utc = now_utc - timedelta(hours=total_hours)

    all_raw_slices = []
    current_start = earliest_utc

    step = timedelta(hours=24)
    slices: List[tuple[datetime, datetime]] = []

    while current_start < now_utc:
        current_end = min(current_start + step, now_utc)
        slices.append((current_start, current_end))
        current_start = current_end

    print(
        f"Requesting {total_hours:.1f} hours ({total_hours / 24:.1f} days) across {len(slices)} API slice(s)..."
    )

    for idx, (s_dt, e_dt) in enumerate(slices, 1):
        print(
            f"  [{idx}/{len(slices)}] Querying: {s_dt.strftime('%Y-%m-%d %H:%M')} -> {e_dt.strftime('%Y-%m-%d %H:%M')} UTC"
        )
        try:
            raw_slice = fetch_history_slice(api_key, device_sn, s_dt, e_dt)
            all_raw_slices.append(raw_slice)
        except Exception as exc:
            print(f"    Warning: Slice {idx} failed: {exc}")

        if idx < len(slices):
            time_mod.sleep(0.5)

    return all_raw_slices


def transform_history_to_silver(
    raw_responses: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Pivots Fox ESS historical nested arrays into aligned tabular records."""
    records_by_time: Dict[str, Dict[str, Any]] = {}

    for raw in raw_responses:
        results = raw.get("result", [])
        if not results or not isinstance(results, list):
            continue

        device_sn = results[0].get("deviceSN", "unknown_device")
        inverter_surrogate = (
            f"inv_{hashlib.sha256(device_sn.encode('utf-8')).hexdigest()[:12]}"
        )
        datas = results[0].get("datas", [])

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
                    # Proper UK timezone handling to avoid double-offset drift
                    if parsed_dt.tzinfo is None:
                        local_dt = parsed_dt.replace(tzinfo=UK_TZ)
                    else:
                        local_dt = parsed_dt.astimezone(UK_TZ)
                    utc_dt = local_dt.astimezone(timezone.utc)

                    records_by_time[raw_time] = {
                        "reading_timestamp_utc": utc_dt.isoformat(),
                        "reading_timestamp_local": local_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        "inverter_id": inverter_surrogate,
                        "pv_power_kw": 0.0,
                        "loads_power_kw": 0.0,
                        "grid_consumption_kw": 0.0,
                        "grid_feed_in_kw": 0.0,
                        "battery_charge_kw": 0.0,
                        "battery_discharge_kw": 0.0,
                        "battery_soc_pct": 0.0,
                        "battery_temp_celsius": 0.0,
                        "cum_generation_kwh": None,
                        "cum_grid_import_kwh": None,
                        "cum_grid_export_kwh": None,
                        "cum_loads_kwh": None,
                        "cum_battery_charge_kwh": None,
                        "cum_battery_discharge_kwh": None,
                    }

                col_name = VARIABLE_MAPPING.get(var_name)
                if col_name and raw_val is not None:
                    records_by_time[raw_time][col_name] = round(
                        float(raw_val), 3
                    )

    return list(records_by_time.values())


def is_off_peak(local_time_str: str) -> bool:
    """Checks whether local time falls within Octopus Go 00:30-05:30 off-peak window."""
    parsed_dt = dt_parser.parse(local_time_str)
    check_time = parsed_dt.time()
    return (
        OCTOPUS_GO_CONFIG["off_peak_start"]
        <= check_time
        < OCTOPUS_GO_CONFIG["off_peak_end"]
    )


def compute_interval_metrics_and_costs(
    sorted_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Calculates deltas from cumulative meters (or trapezoidal power) and evaluates Octopus Go costs."""
    for i in range(len(sorted_records)):
        curr = sorted_records[i]
        local_iso = curr["reading_timestamp_local"]
        off_peak = is_off_peak(local_iso)

        import_rate = (
            OCTOPUS_GO_CONFIG["off_peak_rate_p_kwh"]
            if off_peak
            else OCTOPUS_GO_CONFIG["peak_rate_p_kwh"]
        )
        export_rate = OCTOPUS_GO_CONFIG["export_rate_p_kwh"]

        fallback_import_kwh = curr.get("grid_consumption_kw", 0.0) / 12.0
        fallback_export_kwh = curr.get("grid_feed_in_kw", 0.0) / 12.0
        fallback_gen_kwh = curr.get("pv_power_kw", 0.0) / 12.0
        fallback_load_kwh = curr.get("loads_power_kw", 0.0) / 12.0

        if i == 0:
            delta_import = fallback_import_kwh
            delta_export = fallback_export_kwh
            delta_gen = fallback_gen_kwh
            delta_load = fallback_load_kwh
        else:
            prev = sorted_records[i - 1]

            def get_delta(curr_key: str, fallback_val: float) -> float:
                c_val = curr.get(curr_key)
                p_val = prev.get(curr_key)
                if (
                    c_val is not None
                    and p_val is not None
                    and (c_val - p_val) >= 0
                ):
                    return c_val - p_val
                return fallback_val

            delta_import = get_delta("cum_grid_import_kwh", fallback_import_kwh)
            delta_export = get_delta("cum_grid_export_kwh", fallback_export_kwh)
            delta_gen = get_delta("cum_generation_kwh", fallback_gen_kwh)
            delta_load = get_delta("cum_loads_kwh", fallback_load_kwh)

        self_consumed_kwh = max(0.0, delta_load - delta_import)

        cost_import_gbp = (delta_import * import_rate) / 100.0
        revenue_export_gbp = (delta_export * export_rate) / 100.0
        savings_solar_gbp = (self_consumed_kwh * import_rate) / 100.0

        curr.update(
            {
                "tariff_window": "off_peak" if off_peak else "peak",
                "unit_rate_p_kwh": import_rate,
                "export_rate_p_kwh": export_rate,
                "delta_grid_import_kwh": round(delta_import, 4),
                "delta_grid_export_kwh": round(delta_export, 4),
                "delta_generation_kwh": round(delta_gen, 4),
                "delta_load_kwh": round(delta_load, 4),
                "delta_self_consumed_kwh": round(self_consumed_kwh, 4),
                "cost_import_gbp": round(cost_import_gbp, 4),
                "revenue_export_gbp": round(revenue_export_gbp, 4),
                "savings_solar_gbp": round(savings_solar_gbp, 4),
            }
        )

    return sorted_records


def merge_and_persist(new_records: List[Dict[str, Any]], lookback_hours: float, start_time_mono: float) -> int:
    """
    Merges incoming telemetry records idempotently with historical data,
    executing Phase 1b verification before committing to disk.
    """
    existing_records: Dict[str, Dict[str, Any]] = {}

    if OUTPUT_FILE_PATH.exists():
        try:
            with open(OUTPUT_FILE_PATH, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                for item in loaded:
                    existing_records[item["reading_timestamp_utc"]] = item
        except Exception as exc:
            print(f"Warning: Could not parse existing dataset: {exc}")

    # Track how many brand-new timestamps arrived in this batch
    initial_keys = set(existing_records.keys())
    for item in new_records:
        existing_records[item["reading_timestamp_utc"]] = item
    new_records_count = len(set(existing_records.keys()) - initial_keys)

    sorted_dataset = [
        existing_records[k] for k in sorted(existing_records.keys())
    ]
    enriched_dataset = compute_interval_metrics_and_costs(sorted_dataset)

    # --- PHASE 1b: VERIFICATION & DATA CONTRACT GATE ---
    print("\n--- Running Data Quality & Invariant Verification Gate ---")
    verification = data_quality.verify_dataset_invariants(enriched_dataset)
    verification.new_records_count = new_records_count

    latest_utc = enriched_dataset[-1]["reading_timestamp_utc"] if enriched_dataset else ""
    duration_sec = time_mod.monotonic() - start_time_mono

    # Record operational health status to data/ops/run_status.json
    data_quality.record_pipeline_run_status(
        verification=verification,
        execution_duration_sec=duration_sec,
        lookback_hours=lookback_hours,
        latest_record_utc=latest_utc,
    )

    # Circuit Breaker: Halt commit if schema or physical boundaries failed critically
    if not verification.passed:
        print("[CRITICAL] Data Quality Gate FAILED. Halting commit to prevent database corruption.")
        raise RuntimeError("Data contract violations detected; pipeline halted by circuit breaker.")

    OUTPUT_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE_PATH, "w", encoding="utf-8") as f:
        json.dump(enriched_dataset, f, indent=2)

    return len(enriched_dataset)


def parse_duration_hours() -> float:
    """Resolves trailing window duration using CLI arguments, environment variables, or defaults."""
    parser = argparse.ArgumentParser(
        description="Fox ESS Solar Telemetry Pipeline"
    )
    parser.add_argument(
        "--days", type=float, help="Number of trailing days to ingest/backfill"
    )
    parser.add_argument(
        "--hours",
        type=float,
        help="Number of trailing hours to ingest/backfill",
    )
    args, _ = parser.parse_known_args()

    if args.days is not None:
        return args.days * 24.0
    if args.hours is not None:
        return args.hours

    env_days = os.getenv("BACKFILL_DAYS")
    if env_days:
        try:
            return float(env_days) * 24.0
        except ValueError:
            pass

    env_hours = os.getenv("BACKFILL_HOURS")
    if env_hours:
        try:
            return float(env_hours)
        except ValueError:
            pass

    return 4.0


def main() -> None:
    start_time_mono = time_mod.monotonic()
    api_key = os.getenv("FOX_API_KEY")
    device_sn = os.getenv("FOX_DEVICE_SN")

    if not api_key or not device_sn:
        print(
            f"Error: FOX_API_KEY or FOX_DEVICE_SN not set in {PROJECT_ROOT / '.env'}."
        )
        sys.exit(1)

    hours_to_fetch = parse_duration_hours()
    raw_slices = fetch_historical_window(api_key, device_sn, hours_to_fetch)
    silver_batch = transform_history_to_silver(raw_slices)

    total_count = merge_and_persist(silver_batch, hours_to_fetch, start_time_mono)
    print(f"\nTarget Path: {OUTPUT_FILE_PATH}")
    print(
        f"Pipeline complete. Master dataset now contains {total_count} enriched 5-minute records."
    )


if __name__ == "__main__":
    main()
