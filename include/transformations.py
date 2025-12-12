"""Utilities to clean daily transaction files for the medallion pipeline."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd

RAW_FILE_TEMPLATE = "transactions_{ds_nodash}.csv"
CLEAN_FILE_TEMPLATE = "transactions_{ds_nodash}_clean.parquet"


def _coerce_amount(value: pd.Series) -> pd.Series:
    """Normalize numeric fields and drop non-coercible entries."""
    return pd.to_numeric(value, errors="coerce")


def _normalize_status(value: pd.Series) -> pd.Series:
    """Normalize status values to a standard set."""
    normalized = value.fillna("").str.strip().str.lower()
    mapping = {
        "completed": "completed",
        "pending": "pending",
        "failed": "failed",
    }
    return normalized.map(mapping)


def clean_daily_transactions(
    execution_date: date,
    raw_dir: Path,
    clean_dir: Path,
    raw_template: str = RAW_FILE_TEMPLATE,
    clean_template: str = CLEAN_FILE_TEMPLATE,
) -> Path:
    """
    Clean the raw CSV for the DAG date and save as parquet.

    Archivo utilizado:
    - Si existe el CSV exacto para execution_date → usarlo.
    - Si NO existe:
        • Buscar archivos <= execution_date.
        • Si el archivo más reciente tiene delta <= 1 día → usarlo.
        • Si delta > 1 día → generar CSV sintético vacío.
    - Si no existe ningún CSV → generar CSV sintético vacío.
    """

    ds_nodash = execution_date.strftime("%Y%m%d")
    expected_path = raw_dir / raw_template.format(ds_nodash=ds_nodash)

    # Si el archivo del día existe → usarlo
    if expected_path.exists():
        input_path = expected_path
    else:
        raw_dir.mkdir(parents=True, exist_ok=True)

        # Buscar archivos raw válidos
        candidates = []
        for path in raw_dir.glob("transactions_*.csv"):
            try:
                stem = path.stem  # "transactions_20251209"
                date_str = stem.split("_")[1]
                file_date = datetime.strptime(date_str, "%Y%m%d").date()
                if file_date <= execution_date:
                    candidates.append((path, file_date))
            except Exception:
                continue

        # Si hay archivos previos...
        if candidates:
            latest_path, latest_date = max(candidates, key=lambda x: x[1])
            delta_days = (execution_date - latest_date).days

            if delta_days <= 1:
                print(
                    f"[clean_daily_transactions] INFO: No se encontró "
                    f"{expected_path.name}. Usando archivo anterior: "
                    f"{latest_path.name} (delta {delta_days} días)."
                )
                input_path = latest_path
            else:
                print(
                    f"[clean_daily_transactions] WARNING: No hay archivo para "
                    f"{execution_date}, y el último existente ({latest_date}) "
                    f"tiene delta de {delta_days} días. Se generará CSV vacío."
                )
                df_empty = pd.DataFrame(
                    columns=[
                        "transaction_id",
                        "customer_id",
                        "amount",
                        "status",
                        "transaction_ts",
                    ]
                )
                df_empty.to_csv(expected_path, index=False)
                input_path = expected_path

        else:
            print(
                f"[clean_daily_transactions] WARNING: No existen archivos raw "
                f"para fechas <= {execution_date}. Se generará CSV vacío."
            )
            df_empty = pd.DataFrame(
                columns=[
                    "transaction_id",
                    "customer_id",
                    "amount",
                    "status",
                    "transaction_ts",
                ]
            )
            df_empty.to_csv(expected_path, index=False)
            input_path = expected_path

    # Ruta del archivo limpio generado
    output_path = clean_dir / clean_template.format(ds_nodash=ds_nodash)
    clean_dir.mkdir(parents=True, exist_ok=True)

    # Leer el CSV seleccionado
    df = pd.read_csv(input_path)

    # Limpiezas básicas
    df.columns = [col.strip().lower() for col in df.columns]
    df = df.drop_duplicates()

    if "amount" in df.columns:
        df["amount"] = _coerce_amount(df["amount"])

    if "status" in df.columns:
        df["status"] = _normalize_status(df["status"])

    df = df.dropna(subset=["transaction_id", "customer_id", "amount", "status"])

    # Campos derivados
    if "transaction_ts" in df.columns:
        df["transaction_ts"] = pd.to_datetime(df["transaction_ts"], errors="coerce")
        df = df.dropna(subset=["transaction_ts"])
        df["transaction_date"] = df["transaction_ts"].dt.date

    # Guardar parquet
    df.to_parquet(output_path, index=False)

    return output_path

