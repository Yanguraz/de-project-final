import json
import boto3
import pendulum
import vertica_python
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.python import PythonOperator


AWS_ACCESS_KEY_ID = Variable.get("aws_access_key_id")
AWS_SECRET_ACCESS_KEY = Variable.get("aws_secret_access_key")


conn_info = {
    "host": Variable.get("vertica_host"),
    "port": Variable.get("vertica_port"),
    "user": Variable.get("vertica_user"),
    "password": Variable.get("vertica_password"),
    "autocommit": True,
}


def fetch_s3_file(bucket: str, key: str):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name="s3",
        endpoint_url="https://storage.yandexcloud.net",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(Bucket=bucket, Key=key, Filename=f"/data/{key}")

def load_currencies_staging(conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(
            """COPY STV2024031225__STAGING.currencies
            (currency_code, currency_code_with, date_update, currency_with_div)
            FROM LOCAL '/data/currencies_history.csv' DELIMITER ','""",
            buffer_size=65536,
        )
        return cur.fetchall()

def load_transactions_staging(file_num, conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(
            f"""COPY STV2024031225__STAGING.transactions
            (operation_id, account_number_from, account_number_to, currency_code,
            country, status, transaction_type, amount, transaction_dt)
            FROM LOCAL '/data/transactions_batch_{file_num}.csv' DELIMITER ','""",
            buffer_size=65536,
        )
        return cur.fetchall()

@dag(
    schedule="0 12 1 * *",
    start_date=pendulum.parse("2022-10-01"),
    catchup=False,
)
def stg_dag():
    """Define the Airflow DAG for staging data."""
    fetch_currencies_task = PythonOperator(
        task_id="fetch_currencies",
        python_callable=fetch_s3_file,
        op_kwargs={"bucket": "final-project", "key": "currencies_history.csv"},
    )

    load_currencies_task = PythonOperator(
        task_id="load_currencies_staging",
        python_callable=load_currencies_staging,
    )

    fetch_transactions_tasks = []
    load_transactions_tasks = []
    
    for i in range(1, 2):
        fetch_task = PythonOperator(
            task_id=f"fetch_transactions_{i}",
            python_callable=fetch_s3_file,
            op_kwargs={"bucket": "final-project", "key": f"transactions_batch_{i}.csv"},
        )
        fetch_transactions_tasks.append(fetch_task)

        load_task = PythonOperator(
            task_id=f"load_transactions_staging_{i}",
            python_callable=load_transactions_staging,
            op_kwargs={"file_num": i},
        )
        load_transactions_tasks.append(load_task)

    (fetch_currencies_task
        >> fetch_transactions_tasks
        >> load_currencies_task
        >> load_transactions_tasks)

# Instantiate the DAG 
_ = stg_dag()
