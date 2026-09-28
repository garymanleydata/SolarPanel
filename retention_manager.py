"""
Declarative Data Retention Engine.
Enforces lifecycle policies defined across config/data_catalog.json and config/retention_rules.json.
Supports time-based pruning (days, years), record caps, and generates audit logs in data/ops/retention_status.json.
"""

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import time as time_mod
from typing import Any, Dict, List, Optional, Tuple
from dateutil import parser as dt_parser

# Project directory structure
PROJECT_ROOT = Path(__file__).resolve().parent
CONFIG_DIR = PROJECT_ROOT / "config"
OPS_DIR = PROJECT_ROOT / "data" / "ops"
CATALOG_FILE = CONFIG_DIR / "data_catalog.json"
RULES_FILE = CONFIG_DIR / "retention_rules.json"
RETENTION_STATUS_FILE = OPS_DIR / "retention_status.json"


def load_json_file(file_path: Path) -> Any:
    """Reads JSON payload from a file path with defensive validation."""
    if not file_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_governance_spec() -> Tuple[List[Dict[str, Any]], Dict[Tuple[str, str], Dict[str, Any]]]:
    """
    Loads data catalog and indexes retention rules by (layer, grain) tuple.
    Enables O(1) declarative policy lookup for any table.
    """
    catalog_data = load_json_file(CATALOG_FILE)
    rules_data = load_json_file(RULES_FILE)

    tables = catalog_data.get("tables", [])
    raw_rules = rules_data.get("rules", [])

    rule_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in raw_rules:
        key = (r.get("layer", "").lower(), r.get("grain", "").lower())
        rule_map[key] = r

    return tables, rule_map


def parse_record_datetime(val: Any, date_format: str) -> Optional[datetime]:
    """
    Converts heterogeneous record date fields into timezone-aware UTC datetimes.
    Supports ISO 8601 timestamps and calendar YYYY-MM-DD date strings.
    """
    if not val:
        return None

    try:
        if date_format == "yyyy-mm-dd":
            clean_str = str(val).strip()[:10]
            dt = datetime.strptime(clean_str, "%Y-%m-%d")
            return dt.replace(tzinfo=timezone.utc)
        
        # Default ISO / UTC parser
        parsed = dt_parser.parse(str(val))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def calculate_retention_cutoff(strategy: str, retention_value: Optional[int], ref_now: datetime) -> Optional[datetime]:
    """Computes the strict time boundary for time-based retention strategies."""
    if strategy == "rolling_days" and retention_value is not None:
        return ref_now - timedelta(days=retention_value)
    if strategy == "rolling_years" and retention_value is not None:
        # Uses 365.25 days to maintain leap year accuracy across multi-year retention
        return ref_now - timedelta(days=int(retention_value * 365.25))
    return None


def prune_table_records(
    records: List[Dict[str, Any]],
    table_meta: Dict[str, Any],
    rule_meta: Dict[str, Any],
    now_utc: datetime
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Applies the declarative retention rule to an in-memory dataset.
    Returns the pruned dataset along with audit telemetry.
    """
    table_id = table_meta["table_id"]
    strategy = rule_meta.get("strategy", "full_history")
    retention_val = rule_meta.get("retention_value")
    date_field = table_meta.get("date_field")
    date_format = table_meta.get("date_format", "iso_utc")

    original_count = len(records)
    if original_count == 0:
        return [], {
            "table_id": table_id,
            "strategy": strategy,
            "original_count": 0,
            "pruned_count": 0,
            "retained_count": 0,
            "oldest_record_before": None,
            "oldest_record_after": None,
            "status": "EMPTY"
        }

    # Extract oldest and newest timestamps before pruning
    dts_before = [
        parse_record_datetime(r.get(date_field), date_format)
        for r in records
        if r.get(date_field)
    ]
    valid_dts_before = [d for d in dts_before if d is not None]
    oldest_before = min(valid_dts_before).isoformat() if valid_dts_before else None
    newest_before = max(valid_dts_before).isoformat() if valid_dts_before else None

    retained_records: List[Dict[str, Any]] = []

    # STRATEGY 1: Full History (immutable master layer)
    if strategy == "full_history":
        retained_records = list(records)

    # STRATEGY 2: Maximum Records Cap (e.g. operational logs)
    elif strategy == "max_records" and retention_val is not None:
        max_rows = int(retention_val)
        if original_count <= max_rows:
            retained_records = list(records)
        else:
            # Check ordering direction (descending vs ascending)
            first_dt = valid_dts_before[0] if valid_dts_before else None
            last_dt = valid_dts_before[-1] if valid_dts_before else None

            if first_dt and last_dt and first_dt >= last_dt:
                # Newest records are at the beginning (e.g., ops run history)
                retained_records = records[:max_rows]
            else:
                # Newest records are at the end (chronological ascending)
                retained_records = records[-max_rows:]

    # STRATEGY 3: Time-Window Boundary (rolling days / rolling years)
    elif strategy in ("rolling_days", "rolling_years"):
        cutoff_dt = calculate_retention_cutoff(strategy, retention_val, now_utc)
        if cutoff_dt is None:
            retained_records = list(records)
        else:
            for r in records:
                rec_dt = parse_record_datetime(r.get(date_field), date_format)
                # Keep records that are >= cutoff_dt, or records missing timestamps defensively
                if rec_dt is None or rec_dt >= cutoff_dt:
                    retained_records.append(r)

    else:
        # Default safety fallback: retain without destructive modification
        retained_records = list(records)

    # Calculate post-pruning metrics
    dts_after = [
        parse_record_datetime(r.get(date_field), date_format)
        for r in retained_records
        if r.get(date_field)
    ]
    valid_dts_after = [d for d in dts_after if d is not None]
    oldest_after = min(valid_dts_after).isoformat() if valid_dts_after else None

    pruned_count = original_count - len(retained_records)

    audit_entry = {
        "table_id": table_id,
        "layer": table_meta.get("layer"),
        "grain": table_meta.get("grain"),
        "strategy": strategy,
        "retention_value": retention_val,
        "original_count": original_count,
        "pruned_count": pruned_count,
        "retained_count": len(retained_records),
        "oldest_record_before": oldest_before,
        "oldest_record_after": oldest_after,
        "newest_record": newest_before,
        "status": "PRUNED" if pruned_count > 0 else "COMPLIANT"
    }

    return retained_records, audit_entry


def persist_json_atomically(file_path: Path, data: Any) -> None:
    """Safely writes JSON payload via an atomic filesystem swap to prevent partial writes."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = file_path.with_suffix(".tmp")

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    tmp_path.replace(file_path)


def execute_retention_lifecycle(dry_run: bool = False) -> Dict[str, Any]:
    """
    Orchestrates policy execution across all registered catalog tables.
    Writes updated tables and records operational compliance audit log.
    """
    start_mono = time_mod.monotonic()
    now_utc = datetime.now(timezone.utc)

    print("=" * 70)
    print(f"Data Retention & Governance Manager {'[DRY-RUN]' if dry_run else ''}")
    print(f"Timestamp: {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 70)

    tables, rule_map = load_governance_spec()
    print(f"Loaded {len(tables)} tables from catalog and {len(rule_map)} active retention rules.\n")

    audit_results: List[Dict[str, Any]] = []
    total_pruned_records = 0

    for table_meta in tables:
        table_id = table_meta["table_id"]
        rel_path = table_meta["file_path"]
        file_path = PROJECT_ROOT / rel_path
        layer = table_meta.get("layer", "").lower()
        grain = table_meta.get("grain", "").lower()

        rule_key = (layer, grain)
        rule = rule_map.get(rule_key)

        if not rule:
            print(f"[*] {table_id} ({layer}/{grain}): No rule mapped. Skipping.")
            continue

        if not file_path.exists():
            print(f"[-] {table_id}: File not present ({rel_path}). Skipping.")
            audit_results.append({
                "table_id": table_id,
                "file_path": rel_path,
                "status": "FILE_NOT_FOUND"
            })
            continue

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as exc:
            print(f"[!] {table_id}: Failed to parse JSON ({exc}). Skipping.")
            audit_results.append({
                "table_id": table_id,
                "file_path": rel_path,
                "status": "CORRUPT_JSON",
                "error": str(exc)
            })
            continue

        if not isinstance(data, list):
            print(f"[!] {table_id}: Expected JSON array of records. Skipping.")
            continue

        retained_data, audit_entry = prune_table_records(data, table_meta, rule, now_utc)
        audit_entry["file_path"] = rel_path
        audit_results.append(audit_entry)

        pruned = audit_entry["pruned_count"]
        total_pruned_records += pruned

        print(
            f"[{'PRUNED' if pruned > 0 else 'OK'}] {table_id:<26} | "
            f"Strategy: {audit_entry['strategy']:<13} | "
            f"Before: {audit_entry['original_count']:<6} | "
            f"Pruned: {pruned:<5} | "
            f"Retained: {audit_entry['retained_count']}"
        )

        if pruned > 0 and not dry_run:
            persist_json_atomically(file_path, retained_data)

    duration = round(time_mod.monotonic() - start_mono, 3)

    summary_log = {
        "executed_at_utc": now_utc.isoformat(),
        "dry_run": dry_run,
        "execution_duration_seconds": duration,
        "total_tables_evaluated": len(tables),
        "total_records_pruned": total_pruned_records,
        "compliance_status": "COMPLIANT",
        "table_audits": audit_results
    }

    if not dry_run:
        persist_json_atomically(RETENTION_STATUS_FILE, summary_log)
        print(f"\nRetention audit persisted to {RETENTION_STATUS_FILE.relative_to(PROJECT_ROOT)}")

    print(f"\nExecution completed in {duration}s. Total records pruned: {total_pruned_records}.")
    print("=" * 70)
    return summary_log


def main() -> None:
    parser = argparse.ArgumentParser(description="Declarative Data Lakehouse Retention Manager")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate retention rules and print planned deletions without modifying persistent files."
    )
    args = parser.parse_args()

    execute_retention_lifecycle(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
