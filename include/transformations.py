"""Utilities to clean daily transaction files for the medallion pipeline."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pendulum

RAW_FILE_TEMPLATE = "transactions_{ds_nodash}.csv"
CLEAN_FILE_TEMPLATE = "transactions_{ds_nodash}_clean.parquet"


def _coerce_amount(value: pd.Series) -> pd.Series:
    """Normalize numeric fields and drop non-coercible entries."""
    coerced = pd.to_numeric(value, errors="coerce")
    return coerced


def _normalize_status(value: pd.Series) -> pd.Series:
    normalized = value.fillna("").str.strip().str.lower()
    mapping = {
        "completed": "completed",
        "pending": "pending",
        "failed": "failed",
    }
    return normalized.map(mapping)


def clean_daily_transactions(
    raw_dir: Path,
    clean_dir: Path,
    raw_template: str = RAW_FILE_TEMPLATE,
    clean_template: str = CLEAN_FILE_TEMPLATE,
    **context,
) -> Path:
    """Read the raw CSV for the DAG date, clean it, and save a parquet file."""
    # Get the execution date from context
    data_interval_start = context.get("data_interval_start")
    if data_interval_start:
        ds_nodash = data_interval_start.format("YYYYMMDD")
    else:
        logical_date = (
            context.get("logical_date")
            or context.get("execution_date")
            or context.get("data_interval_start")
            or context.get("run_id")  # Last resort, parse from run_id
        )
        if logical_date is None:
            logical_date = pendulum.now("UTC")

        # Handle both pendulum and datetime objects
        if hasattr(logical_date, "strftime"):
            ds_nodash = logical_date.strftime("%Y%m%d")
        else:
            # Fallback: use today's date
            ds_nodash = context.get("ds_nodash")
            if not ds_nodash:
                ds_nodash = pendulum.now("UTC").strftime("%Y%m%d")

    input_path = raw_dir / raw_template.format(ds_nodash=ds_nodash)
    output_path = clean_dir / clean_template.format(ds_nodash=ds_nodash)

    if not input_path.exists():
        raise FileNotFoundError(f"Raw data not found for {ds_nodash}: {input_path}")

    clean_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(input_path)

    # Basic cleanup
    df.columns = [col.strip().lower() for col in df.columns]
    df = df.drop_duplicates()

    if "amount" in df.columns:
        df["amount"] = _coerce_amount(df["amount"])

    if "status" in df.columns:
        df["status"] = _normalize_status(df["status"])

    df = df.dropna(subset=["transaction_id", "customer_id", "amount", "status"])

    # Add simple derived fields for downstream dbt modeling
    if "transaction_ts" in df.columns:
        df["transaction_ts"] = pd.to_datetime(df["transaction_ts"], errors="coerce")
        df = df.dropna(subset=["transaction_ts"])
        df["transaction_date"] = df["transaction_ts"].dt.date

    df.to_parquet(output_path, index=False)

    return str(output_path)
