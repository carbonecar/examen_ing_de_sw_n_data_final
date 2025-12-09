from datetime import timedelta
from airflow.sdk import DAG
import pendulum

with DAG(
    description="Bronze/Silver/Gold medallion demo with pandas, dbt, and DuckDB",
    dag_id="medallion_pipeline_new",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
) as medallion_dag:
    # Aquí es donde se definirán las tareas del DAG.
    # Se deben crear tareas para cada etapa del pipeline: bronze, silver y gold.
    # Cada tarea debe usar las funciones definidas arriba para procesar los datos.
    #
    # Pistas:
    #  * Usar PythonOperator para definir las tareas que ejecutan funciones Python.
    #  * Asegurarse de que las tareas dependan de la variable ds_nodash para que los archivos
    #    de entrada y salida sean específicos para la fecha de ejecución.
    #  * Definir las dependencias entre tareas para que se ejecuten en el orden correcto:
    #    bronze -> silver -> gold.
    pass

    # Ejemplo de definición de una tarea usando PythonOperator
    # from airflow.operators.python import PythonOperator
    # bronze_task = PythonOperator(
    #     task_id="bronze_task",
    #     python_callable=clean_daily_transactions,
    #     op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
    # )

    # Definir las dependencias entre tareas
    # bronze_task >> silver_task >> gold_task
