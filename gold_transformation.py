"""
Gold Tier Transformation Engine.
Reads Master Silver telemetry (data/silver/telemetry_5min.json) and compiles:
  1. Gold Table 1 (telemetry_recent_30d.json): High-res 5-minute rolling 30-day window for responsive charts.
  2. Gold Table 2 (daily_rollups.json): Pre-aggregated daily energy ledger & financial metrics for 5-year retention.
"""

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import time as time_mod
from typing import Any, Dict, List
from zoneinfo import ZoneInfo
from dateutil import parser as dt_parser

PROJECT_ROOT = Path(__file__).resolve().parent
SILVER_FILE = PROJECT_ROOT / "data" / "silver" / "telemetry_5min.json"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
OPS_DIR = PROJECT_ROOT / "data" / "ops"
GOLD_RECENT_30D_FILE = GOLD_DIR / "telemetry_recent_30d.json"
GOLD_DAILY_ROLLUPS_FILE = GOLD_DIR / "daily_rollups.json"
GOLD_STATUS_FILE = OPS_DIR / "gold_status.json"

# Fixed daily standing charge in GBP (52.0p/day on Octopus Go Region J)
DAILY_STANDING_CHARGE_GBP = 0.5200
UK_TZ = ZoneInfo("Europe/London")

from dateutil import parser as dt_parser

PROJECT_ROOT = Path(__file__).resolve().parent
SILVER_FILE = PROJECT_ROOT / "data" / "silver" / "telemetry_5min.json"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
GOLD_RECENT_30D_FILE = GOLD_DIR / "telemetry_recent_30d.json"
GOLD_DAILY_ROLLUPS_FILE = GOLD_DIR / "daily_rollups.json"

# Fixed daily standing charge in GBP (52.0p/day on Octopus Go Region J)
DAILY_STANDING_CHARGE_GBP = 0.5200


def load_silver_telemetry() -> List[Dict[str, Any]]:
    """Loads validated historical 5-minute telemetry from the Silver tier."""
    if not SILVER_FILE.exists():
        print(f"[Error] Silver telemetry master file not found: {SILVER_FILE}")
        sys.exit(1)

    try:
        with open(SILVER_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, list):
                print(f"[Error] Expected a JSON array in {SILVER_FILE}")
                sys.exit(1)
            return data
    except Exception as exc:
        print(f"[Error] Failed to read Silver telemetry: {exc}")
        sys.exit(1)


def generate_gold_recent_30d(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Filters 5-minute records to the rolling 30-day window based on the latest available timestamp.
    Powers fast, low-bandwidth browser rendering on the main dashboard.
    """
    if not records:
        return []

    # Parse latest timestamp from dataset to establish the window ceiling
    latest_ts_raw = records[-1].get("reading_timestamp_utc")
    try:
        latest_dt = dt_parser.parse(latest_ts_raw)
    except Exception:
        latest_dt = datetime.now(timezone.utc)

    # 30-day retention cutoff boundary
    cutoff_dt = latest_dt - timedelta(days=30)
    cutoff_iso = cutoff_dt.isoformat()

    recent_records = [
        r for r in records
        if r.get("reading_timestamp_utc", "") >= cutoff_iso
    ]

    return recent_records


def generate_gold_daily_rollups(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Aggregates interval data into daily ledger rows with physical totals,
    self-consumption autonomy, and financial balance.
    """
    grouped_by_day: Dict[str, List[Dict[str, Any]]] = {}

    for r in records:
        # Group by local date key (YYYY-MM-DD)
        local_ts = r.get("reading_timestamp_local") or r.get("reading_timestamp_utc") or ""
        day_key = local_ts[:10]
        if not day_key or len(day_key) != 10:
            continue

        if day_key not in grouped_by_day:
            grouped_by_day[day_key] = []
        grouped_by_day[day_key].append(r)

    daily_rows: List[Dict[str, Any]] = []

    for day_str in sorted(grouped_by_day.keys(), reverse=True):
        intervals = grouped_by_day[day_str]

        sum_gen_kwh = sum(i.get("delta_generation_kwh", 0.0) or 0.0 for i in intervals)
        sum_load_kwh = sum(i.get("delta_load_kwh", 0.0) or 0.0 for i in intervals)
        sum_import_kwh = sum(i.get("delta_grid_import_kwh", 0.0) or 0.0 for i in intervals)
        sum_export_kwh = sum(i.get("delta_grid_export_kwh", 0.0) or 0.0 for i in intervals)
        sum_self_consumed_kwh = sum(i.get("delta_self_consumed_kwh", 0.0) or 0.0 for i in intervals)

        # Financial totals
        energy_cost_gbp = sum(i.get("cost_import_gbp", 0.0) or 0.0 for i in intervals)
        export_revenue_gbp = sum(i.get("revenue_export_gbp", 0.0) or 0.0 for i in intervals)
        solar_savings_gbp = sum(i.get("savings_solar_gbp", 0.0) or 0.0 for i in intervals)

        total_import_cost_gbp = energy_cost_gbp + DAILY_STANDING_CHARGE_GBP
        net_balance_gbp = total_import_cost_gbp - export_revenue_gbp

        # Autonomy / Self-sufficiency percentage
        autonomy_pct = (
            min(100.0, max(0.0, ((sum_load_kwh - sum_import_kwh) / sum_load_kwh) * 100.0))
            if sum_load_kwh > 0
            else 100.0
        )

        # Peak solar power and Battery SoC bounds across the day
        peak_solar_kw = max((i.get("pv_power_kw", 0.0) or 0.0 for i in intervals), default=0.0)
        soc_values = [i.get("battery_soc_pct") for i in intervals if i.get("battery_soc_pct") is not None]
        min_soc = min(soc_values) if soc_values else 0
        max_soc = max(soc_values) if soc_values else 0

        daily_rows.append({
            "date": day_str,
            "record_count": len(intervals),
            "generation_kwh": round(sum_gen_kwh, 3),
            "load_kwh": round(sum_load_kwh, 3),
            "grid_import_kwh": round(sum_import_kwh, 3),
            "grid_export_kwh": round(sum_export_kwh, 3),
            "self_consumed_kwh": round(sum_self_consumed_kwh, 3),
            "autonomy_pct": round(autonomy_pct, 1),
            "energy_cost_gbp": round(energy_cost_gbp, 2),
            "standing_charge_gbp": round(DAILY_STANDING_CHARGE_GBP, 2),
            "total_import_cost_gbp": round(total_import_cost_gbp, 2),
            "export_revenue_gbp": round(export_revenue_gbp, 2),
            "solar_savings_gbp": round(solar_savings_gbp, 2),
            "net_balance_gbp": round(net_balance_gbp, 2),
            "peak_solar_kw": round(peak_solar_kw, 2),
            "battery_min_soc_pct": min_soc,
            "battery_max_soc_pct": max_soc
        })

    return daily_rows

def get_expected_day_interval_count(day_str: str) -> tuple[int, str]:
    """
    Computes expected interval count taking UK Daylight Saving Time (DST) into account.
    Standard Day: 24h = 288 intervals (5-min).
    Spring forward (March, 23h): 276 intervals.
    Autumn back (October, 25h): 300 intervals.
    """
    try:
        dt_start = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=UK_TZ)
        dt_end = dt_start + timedelta(days=1)
        # Difference in actual clock hours between midnight-to-midnight in Europe/London
        duration_hours = (dt_end.astimezone(timezone.utc) - dt_start.astimezone(timezone.utc)).total_seconds() / 3600.0
        expected_intervals = int(round(duration_hours * 12))
        reason = "standard"
        if expected_intervals == 276:
            reason = "dst_spring_forward_23h"
        elif expected_intervals == 300:
            reason = "dst_autumn_fall_back_25h"
        return expected_intervals, reason
    except Exception:
        return 288, "default_standard"


def verify_and_reconcile_gold(
    silver_records: List[Dict[str, Any]],
    gold_recent: List[Dict[str, Any]],
    gold_daily: List[Dict[str, Any]],
    execution_duration_sec: float
) -> Dict[str, Any]:
    """
    Evaluates Gold transformation data contracts:
      1. Reconciliation: Silver interval energy sums == Gold daily rollup sums.
      2. Completeness: Interval counts per day evaluated with DST tolerance.
      3. Financial Ledger Consistency: Standing charge + energy cost == total bill.
      4. 30-Day Window: Strict retention adherence.
    """
    warnings: List[str] = []
    faults: List[str] = []

    # 1. Reconciliation Invariant (Silver vs Gold Energy Sums)
    silver_gen = sum(r.get("delta_generation_kwh", 0.0) or 0.0 for r in silver_records)
    gold_gen = sum(d.get("generation_kwh", 0.0) for d in gold_daily)
    diff_gen = abs(silver_gen - gold_gen)

    silver_imp = sum(r.get("delta_grid_import_kwh", 0.0) or 0.0 for r in silver_records)
    gold_imp = sum(d.get("grid_import_kwh", 0.0) for d in gold_daily)
    diff_imp = abs(silver_imp - gold_imp)

    # Tolerance: < 0.05 kWh accumulated across the entire multi-month dataset
    if diff_gen > 0.05:
        faults.append(f"Solar yield reconciliation mismatch: Silver {silver_gen:.2f} kWh != Gold {gold_gen:.2f} kWh (diff: {diff_gen:.3f})")
    if diff_imp > 0.05:
        faults.append(f"Grid import reconciliation mismatch: Silver {silver_imp:.2f} kWh != Gold {gold_imp:.2f} kWh (diff: {diff_imp:.3f})")

    # 2. Daily Completeness with DST Tolerance
    today_str = datetime.now(UK_TZ).strftime("%Y-%m-%d")
    complete_days = 0
    partial_days = 0

    for d in gold_daily:
        day_date = d["date"]
        rec_count = d["record_count"]
        expected, reason = get_expected_day_interval_count(day_date)

        if day_date == today_str:
            # Current day in progress: Expected to be partial
            partial_days += 1
        elif rec_count == expected:
            complete_days += 1
        elif abs(rec_count - expected) <= 6:
            # Minor drop (e.g. within 30 mins missing due to router restart)
            warnings.append(f"Day {day_date} has minor telemetry gap: {rec_count}/{expected} intervals ({reason})")
            partial_days += 1
        else:
            warnings.append(f"Day {day_date} is partial: {rec_count}/{expected} intervals ({reason})")
            partial_days += 1

    # 3. Financial Equation Check
    for d in gold_daily:
        expected_bill = round(d["energy_cost_gbp"] + d["standing_charge_gbp"], 2)
        actual_bill = round(d["total_import_cost_gbp"], 2)
        if abs(expected_bill - actual_bill) > 0.02:
            faults.append(f"[{d['date']}] Financial ledger bill error: {actual_bill} != energy {d['energy_cost_gbp']} + SC {d['standing_charge_gbp']}")

        expected_net = round(actual_bill - d["export_revenue_gbp"], 2)
        actual_net = round(d["net_balance_gbp"], 2)
        if abs(expected_net - actual_net) > 0.02:
            faults.append(f"[{d['date']}] Financial net position error: {actual_net} != bill {actual_bill} - exp {d['export_revenue_gbp']}")

        if not (0.0 <= d["autonomy_pct"] <= 100.0):
            faults.append(f"[{d['date']}] Autonomy percentage out of bounds: {d['autonomy_pct']}%")

    # 4. 30-Day Window Boundary
    window_days = 0
    if gold_recent:
        first_ts = gold_recent[0].get("reading_timestamp_utc", "")[:10]
        last_ts = gold_recent[-1].get("reading_timestamp_utc", "")[:10]
        try:
            d_start = datetime.strptime(first_ts, "%Y-%m-%d")
            d_end = datetime.strptime(last_ts, "%Y-%m-%d")
            window_days = (d_end - d_start).days
            if window_days > 32:
                warnings.append(f"Gold 30d window exceeds expected range ({window_days} days)")
        except Exception:
            pass

    passed = len(faults) == 0
    status_level = "HEALTHY" if passed and len(warnings) == 0 else "WARNING" if passed else "CRITICAL"

    verification_log = {
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "status_level": status_level,
        "reconciliation_passed": passed,
        "execution_duration_seconds": round(execution_duration_sec, 2),
        "metrics": {
            "silver_total_records": len(silver_records),
            "gold_recent_records": len(gold_recent),
            "gold_daily_days": len(gold_daily),
            "complete_days_count": complete_days,
            "partial_days_count": partial_days,
            "energy_reconciliation_delta_kwh": round(diff_gen, 4),
            "faults_count": len(faults),
            "faults": faults,
            "warnings_count": len(warnings),
            "warnings_sample": warnings[:5]
        }
    }

    return verification_log


def persist_json_atomically(file_path: Path, data: Any) -> None:
    """Writes JSON payload using an atomic swap to protect against partial writes."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = file_path.with_suffix(".tmp")

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    tmp_path.replace(file_path)


def main() -> None:
    start_mono = time_mod.monotonic()
    print("[Gold Transform] Loading Silver master telemetry...")
    silver_records = load_silver_telemetry()
    print(f"[Gold Transform] Ingested {len(silver_records)} raw 5-minute Silver intervals.")

    # 1. Generate Gold Table 1: Recent 30-Day Window
    print("[Gold Transform] Generating Gold Table 1 (telemetry_recent_30d.json)...")
    recent_30d = generate_gold_recent_30d(silver_records)
    persist_json_atomically(GOLD_RECENT_30D_FILE, recent_30d)
    print(f"  -> Saved {len(recent_30d)} 30-day intervals to {GOLD_RECENT_30D_FILE.relative_to(PROJECT_ROOT)}")

    # 2. Generate Gold Table 2: Daily Rollup Ledger
    print("[Gold Transform] Generating Gold Table 2 (daily_rollups.json)...")
    daily_rollups = generate_gold_daily_rollups(silver_records)
    persist_json_atomically(GOLD_DAILY_ROLLUPS_FILE, daily_rollups)
    print(f"  -> Saved {len(daily_rollups)} daily rollup records to {GOLD_DAILY_ROLLUPS_FILE.relative_to(PROJECT_ROOT)}")

    # 3. Gold Verification, DST-aware checks, and Reconciliation
    print("\n--- Running Gold Tier Reconciliation & Verification Gate ---")
    duration = time_mod.monotonic() - start_mono
    verification_log = verify_and_reconcile_gold(silver_records, recent_30d, daily_rollups, duration)

    # Persist audit status to data/ops/gold_status.json
    persist_json_atomically(GOLD_STATUS_FILE, verification_log)

    metrics = verification_log["metrics"]
    print(f"[Gold Verify] Status: {verification_log['status_level']} in {duration:.2f}s")
    print(f"  -> Reconciliation delta: {metrics['energy_reconciliation_delta_kwh']} kWh")
    print(f"  -> Days evaluated: {metrics['gold_daily_days']} ({metrics['complete_days_count']} complete, {metrics['partial_days_count']} partial)")

    if metrics["faults"]:
        print(f"  [!] Faults: {metrics['faults']}")
    if metrics["warnings_sample"]:
        print(f"  [*] Info/Warnings: {metrics['warnings_sample']}")

    print(f"[Gold Transform] Gold status persisted to {GOLD_STATUS_FILE.relative_to(PROJECT_ROOT)}")
    print("[Gold Transform] Gold tier build completed successfully.")


if __name__ == "__main__":
    main()
