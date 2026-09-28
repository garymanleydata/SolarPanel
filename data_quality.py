"""
Data Contracts, Invariant Verification, and Operational Health Logging.
Adheres to Carruthers & Jackson (C&J) Data Quality and Governance standards.
"""

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple
from dateutil import parser as dt_parser

PROJECT_ROOT = Path(__file__).resolve().parent
OPS_DIR = PROJECT_ROOT / "data" / "ops"
RUN_STATUS_FILE = OPS_DIR / "run_status.json"
RUN_HISTORY_FILE = OPS_DIR / "run_history.json"

# --- 1. DATA CONTRACT SPECIFICATION (SILVER LAYER) ---

REQUIRED_SCHEMA_FIELDS = {
    "reading_timestamp_utc": str,
    "reading_timestamp_local": str,
    "pv_power_kw": (int, float),
    "loads_power_kw": (int, float),
    "grid_consumption_kw": (int, float),
    "grid_feed_in_kw": (int, float),
    "battery_charge_kw": (int, float),
    "battery_discharge_kw": (int, float),
    "battery_soc_pct": (int, float),
    "battery_temp_celsius": (int, float),
}

# Domain & Physical Invariant Boundaries
BOUNDS = {
    "battery_soc_min": 0.0,
    "battery_soc_max": 100.0,
    "pv_power_max_kw": 10.0,         # Exceeding 10kW indicates a corrupted multiplier
    "load_power_max_kw": 18.0,       # Max single-phase domestic draw
    "temp_min_celsius": -20.0,
    "temp_max_celsius": 65.0,
}


class VerificationResult:
    def __init__(self):
        self.total_records: int = 0
        self.new_records_count: int = 0
        self.schema_violations: List[str] = []
        self.boundary_violations: List[str] = []
        self.monotonicity_violations: List[str] = []
        self.balance_warnings: int = 0
        self.passed: bool = True
        self.status_level: str = "HEALTHY"  # HEALTHY | WARNING | CRITICAL

    def to_dict(self) -> Dict[str, Any]:
        return {
            "passed": self.passed,
            "status_level": self.status_level,
            "total_records_evaluated": self.total_records,
            "new_records_count": self.new_records_count,
            "schema_violations_count": len(self.schema_violations),
            "schema_violations_sample": self.schema_violations[:5],
            "boundary_violations_count": len(self.boundary_violations),
            "boundary_violations_sample": self.boundary_violations[:5],
            "monotonicity_violations_count": len(self.monotonicity_violations),
            "monotonicity_violations_sample": self.monotonicity_violations[:5],
            "power_balance_warnings_count": self.balance_warnings,
        }


# --- 2. VERIFICATION ENGINE ---

def verify_record_schema(record: Dict[str, Any]) -> List[str]:
    """Verifies that an individual record satisfies the Silver data contract."""
    errors = []
    ts = record.get("reading_timestamp_utc", "unknown")

    for field, expected_type in REQUIRED_SCHEMA_FIELDS.items():
        if field not in record:
            errors.append(f"[{ts}] Missing required field: {field}")
        elif record[field] is None:
            errors.append(f"[{ts}] Null value in mandatory field: {field}")
        elif not isinstance(record[field], expected_type):
            errors.append(
                f"[{ts}] Type mismatch for {field}: expected {expected_type}, got {type(record[field])}"
            )

    return errors


def verify_physical_bounds(record: Dict[str, Any]) -> List[str]:
    """Ensures telemetry values conform to realistic physical laws and hardware specifications."""
    errors = []
    ts = record.get("reading_timestamp_utc", "unknown")

    soc = record.get("battery_soc_pct")
    if soc is not None and not (BOUNDS["battery_soc_min"] <= soc <= BOUNDS["battery_soc_max"]):
        errors.append(f"[{ts}] Battery SoC out of bounds: {soc}% (must be 0-100%)")

    pv = record.get("pv_power_kw")
    if pv is not None and (pv < 0.0 or pv > BOUNDS["pv_power_max_kw"]):
        errors.append(f"[{ts}] Solar power invalid: {pv} kW")

    load = record.get("loads_power_kw")
    if load is not None and (load < 0.0 or load > BOUNDS["load_power_max_kw"]):
        errors.append(f"[{ts}] Home load invalid: {load} kW")

    temp = record.get("battery_temp_celsius")
    if temp is not None and not (BOUNDS["temp_min_celsius"] <= temp <= BOUNDS["temp_max_celsius"]):
        errors.append(f"[{ts}] Battery temperature abnormal: {temp}°C")

    return errors


def verify_dataset_invariants(records: List[Dict[str, Any]]) -> VerificationResult:
    """
    Evaluates the full dataset against C&J quality dimensions:
    Completeness, Validity, Physical Consistency, and Monotonicity.
    """
    result = VerificationResult()
    result.total_records = len(records)

    if not records:
        result.passed = False
        result.status_level = "CRITICAL"
        result.schema_violations.append("Dataset is completely empty.")
        return result

    for i, curr in enumerate(records):
        # 1. Schema check
        s_errs = verify_record_schema(curr)
        result.schema_violations.extend(s_errs)

        # 2. Physical boundary check
        b_errs = verify_physical_bounds(curr)
        result.boundary_violations.extend(b_errs)

        # 3. Energy Balance Conservation Invariant
        # Energy IN (Solar + Grid Import + Battery Discharge) ≈ Energy OUT (Home Load + Grid Export + Battery Charge)
        p_in = curr.get("pv_power_kw", 0.0) + curr.get("grid_consumption_kw", 0.0) + curr.get("battery_discharge_kw", 0.0)
        p_out = curr.get("loads_power_kw", 0.0) + curr.get("grid_feed_in_kw", 0.0) + curr.get("battery_charge_kw", 0.0)
        imbalance = abs(p_in - p_out)
        
        # Power imbalances > 1.8 kW suggest sampling desynchronisation across channels
        if imbalance > 1.8:
            result.balance_warnings += 1

        # 4. Monotonic Register Checks (against previous interval)
        if i > 0:
            prev = records[i - 1]
            cum_keys = [
                ("cum_generation_kwh", "Solar generation"),
                ("cum_grid_import_kwh", "Grid import"),
                ("cum_grid_export_kwh", "Grid export"),
                ("cum_loads_kwh", "Home loads"),
            ]
            for key, label in cum_keys:
                c_val = curr.get(key)
                p_val = prev.get(key)
                if c_val is not None and p_val is not None:
                    # Allow minor floating precision negative jitter (< 0.001)
                    if (c_val - p_val) < -0.001:
                        ts = curr.get("reading_timestamp_utc", "")
                        result.monotonicity_violations.append(
                            f"[{ts}] Negative counter jump in {label}: {p_val} -> {c_val} (diff: {c_val - p_val:.3f} kWh)"
                        )

    # Calculate overall health status
    total_hard_violations = len(result.schema_violations) + len(result.boundary_violations)
    if total_hard_violations > 0:
        result.passed = False
        result.status_level = "CRITICAL"
    elif len(result.monotonicity_violations) > 0 or result.balance_warnings > (len(records) * 0.15):
        result.passed = True
        result.status_level = "WARNING"
    else:
        result.passed = True
        result.status_level = "HEALTHY"

    return result


# --- 3. OPERATIONAL LOGGING (PHASE 2 PERSISTENCE) ---

def record_pipeline_run_status(
    verification: VerificationResult,
    execution_duration_sec: float,
    lookback_hours: float,
    latest_record_utc: str,
) -> Dict[str, Any]:
    """
    Saves structured run logs to data/ops/run_status.json and appends
    to data/ops/run_history.json for the front-end Data Trust Centre.
    """
    OPS_DIR.mkdir(parents=True, exist_ok=True)
    now_utc = datetime.now(timezone.utc)

    # Compute data freshness lag
    freshness_lag_minutes = None
    if latest_record_utc:
        try:
            latest_dt = dt_parser.parse(latest_record_utc)
            freshness_lag_minutes = round((now_utc - latest_dt).total_seconds() / 60.0, 1)
        except Exception:
            pass

    log_entry = {
        "run_timestamp_utc": now_utc.isoformat(),
        "execution_duration_seconds": round(execution_duration_sec, 2),
        "lookback_hours_requested": lookback_hours,
        "latest_telemetry_timestamp_utc": latest_record_utc,
        "telemetry_freshness_lag_minutes": freshness_lag_minutes,
        "health_status": verification.status_level,
        "contract_passed": verification.passed,
        "metrics": verification.to_dict(),
    }

    # Write latest run status
    with open(RUN_STATUS_FILE, "w", encoding="utf-8") as f:
        json.dump(log_entry, f, indent=2)

    # Append to rolling history (max 100 runs retained)
    history = []
    if RUN_HISTORY_FILE.exists():
        try:
            with open(RUN_HISTORY_FILE, "r", encoding="utf-8") as f:
                history = json.load(f)
        except Exception:
            history = []

    # Prepend newest log and cap at 100 entries
    history.insert(0, log_entry)
    history = history[:100]

    with open(RUN_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2)

    print(
        f"[Data Quality] Status: {verification.status_level} | "
        f"Freshness: {freshness_lag_minutes}m lag | "
        f"Records: {verification.total_records} (New: {verification.new_records_count}) | "
        f"Violations: {len(verification.schema_violations) + len(verification.boundary_violations)}"
    )

    return log_entry
