"""Airflow DAG that orchestrates the medallion pipeline."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

# pylint: disable=import-error,wrong-import-position

# Carpeta base del proyecto (un nivel por encima de /dags)
BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from include.transformations import clean_daily_transactions  # noqa: E402

# Rutas usadas en el pipeline
RAW_DIR = BASE_DIR / "data/raw"
CLEAN_DIR = BASE_DIR / "data/clean"
QUALITY_DIR = BASE_DIR / "data/quality"
DBT_DIR = BASE_DIR / "dbt"
PROFILES_DIR = BASE_DIR / "profiles"
WAREHOUSE_PATH = BASE_DIR / "warehouse/medallion.duckdb"


def _build_env(ds_nodash: str) -> dict[str, str]:
    """Arma las variables de entorno necesarias para ejecutar dbt."""
    env = os.environ.copy()
    env.update(
        {
            "DBT_PROFILES_DIR": str(PROFILES_DIR),
            "CLEAN_DIR": str(CLEAN_DIR),
            "DS_NODASH": ds_nodash,
            "DUCKDB_PATH": str(WAREHOUSE_PATH),
        }
    )
    return env


def _run_dbt_command(command: str, ds_nodash: str) -> subprocess.CompletedProcess:
    """Ejecuta un comando de dbt (run o test) y devuelve el proceso."""
    env = _build_env(ds_nodash)
    result = subprocess.run(
        ["dbt", command, "--project-dir", str(DBT_DIR)],
        cwd=DBT_DIR,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    return result

def _bronze_clean_task(ds_nodash: str, **_) -> None:
    """
    Lee el CSV del día desde data/raw, lo limpia con pandas
    y guarda un parquet en data/clean.
    ds_nodash viene en formato YYYYMMDD ({{ ds_nodash }}).
    """
    exec_date = datetime.strptime(ds_nodash, "%Y%m%d").date()

    print(f"[bronze_clean] Ejecutando limpieza para fecha: {exec_date.isoformat()}")
    print(f"[bronze_clean] RAW_DIR: {RAW_DIR}")
    print(f"[bronze_clean] CLEAN_DIR: {CLEAN_DIR}")

    output_path = clean_daily_transactions(
        execution_date=exec_date,
        raw_dir=RAW_DIR,
        clean_dir=CLEAN_DIR,
    )

    print(f"[bronze_clean] Archivo limpio generado en: {output_path}")


def _silver_dbt_run(ds_nodash: str, **_) -> None:
    """Ejecuta dbt run sobre el modelo silver (lee parquet y carga en DuckDB)."""
    print(f"[silver_dbt_run] Ejecutando dbt run para ds_nodash={ds_nodash}")
    result = _run_dbt_command("run", ds_nodash)

    print("[silver_dbt_run] STDOUT:\n", result.stdout)
    print("[silver_dbt_run] STDERR:\n", result.stderr)

    if result.returncode != 0:
        raise AirflowException("dbt run falló. Revisar logs de STDOUT/STDERR.")


def _gold_dbt_tests(ds_nodash: str, **_) -> None:
    """
    Ejecuta dbt test sobre las tablas finales y genera un archivo JSON
    en data/quality indicando si se pasaron los tests.
    """
    print(f"[gold_dbt_tests] Ejecutando dbt test para ds_nodash={ds_nodash}")
    result = _run_dbt_command("test", ds_nodash)

    print("[gold_dbt_tests] STDOUT:\n", result.stdout)
    print("[gold_dbt_tests] STDERR:\n", result.stderr)

    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    output_path = QUALITY_DIR / f"gold_tests_{ds_nodash}.json"

    status = {
        "ds_nodash": ds_nodash,
        "success": result.returncode == 0,
    }

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)

    print(f"[gold_dbt_tests] Resultado de tests guardado en: {output_path}")

    if result.returncode != 0:
        raise AirflowException("dbt test falló. Ver archivo de resultados y logs.")


def build_dag() -> DAG:
    """Construye el DAG de la pipeline medallion (bronze → silver → gold)."""
    with DAG(
        dag_id="medallion_pipeline",
        description="Bronze/Silver/Gold medallion demo with pandas, dbt, and DuckDB",
        schedule="0 6 * * *",  # corre todos los días a las 06:00 UTC
        start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
        catchup=False,        # ✅ solo corre las fechas que dispares, no todo el histórico
        max_active_runs=1,
    ) as dag:

        bronze_clean = PythonOperator(
            task_id="bronze_clean",
            python_callable=_bronze_clean_task,
            op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
        )

        silver_dbt_run = PythonOperator(
            task_id="silver_dbt_run",
            python_callable=_silver_dbt_run,
            op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
        )

        gold_dbt_tests = PythonOperator(
            task_id="gold_dbt_tests",
            python_callable=_gold_dbt_tests,
            op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
        )

        bronze_clean >> silver_dbt_run >> gold_dbt_tests

    return dag


dag = build_dag()


