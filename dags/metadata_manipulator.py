from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

def print_runs(**kwargs):
    print(f'-----\nCONTEXT: {kwargs}\n-----\n')
    ti = kwargs['ti']
    dag_runs = ti.xcom_pull(task_ids="select_dag_runs")
    dag_run = kwargs['dag_run']
    print(f'-----\nALL DAG RUNS: {dag_runs}\n-----\n')
    print(f'-----\nTHIS DAG RUN: {dag_run}\n-----\n')

def write_runs(ti=None):
    dag_runs = ti.xcom_pull(task_ids="select_dag_runs")
    str_dag_runs = [str(dag_run) for dag_run in dag_runs]
    print(str_dag_runs)
    with open("/opt/airflow/output/dagruns.csv", "w") as file:
        for run in str_dag_runs:
            file.write(f"{run}\n")

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

select_dag_runs = PostgresOperator(
    task_id="select_dag_runs",
    postgres_conn_id="metadata-db",
    sql='''
        SELECT dag_id, execution_date, state, run_id
        FROM dag_run
        ORDER BY execution_date DESC
        LIMIT 5;
        ''',
    dag=dag
)

print_dag_runs = PythonOperator(
    task_id = "print_dag_runs",
    python_callable = print_runs,
    dag=dag
)

write_dag_runs = PythonOperator(
    task_id = "write_dag_runs",
    python_callable = write_runs,
    dag=dag
)

select_dag_runs >> print_dag_runs >> write_dag_runs