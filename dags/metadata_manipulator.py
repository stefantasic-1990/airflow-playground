from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator

dag = DAG(
    dag_id = "metadata_manipulator",
    start_date = days_ago(1),
    schedule_interval = "*/5 * * * *",
    catchup = False,
    default_args = {
        "owner": "stefantasic",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": 5.0,
        "provide_context": True
        }
)

PostgresOperator(
    task_id='run_query',
    postgres_conn_id='metadata-db',
    sql="SELECT 1;",
    dag=dag
)