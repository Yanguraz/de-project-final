import json
import pendulum
import vertica_python
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.python import PythonOperator

HOST = Variable.get("vertica_host")
PORT = Variable.get("vertica_port")
USER = Variable.get("vertica_user")
PASSWORD = Variable.get("vertica_password")

conn_info = {
    "host": HOST,
    "port": PORT,
    "user": USER,
    "password": PASSWORD,
    "autocommit": True,
}

def load_global_metrics(snap_date, conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        query = open("/lessons/sql/insert.sql", "r").read()
        query = query.replace('{{ ds }}', snap_date)
        cur.execute(query)
        return cur.fetchall()

@dag(
    schedule_interval="0 12 * * *",
    start_date=pendulum.parse("2022-10-01"),
    catchup=True,
)
def dwh_dag():
    load_global_metrics_task = PythonOperator(
        task_id="load_global_metrics",
        python_callable=load_global_metrics,
        op_kwargs={"snap_date": "{{ ds }}"},
    )

    load_global_metrics_task

_ = dwh_dag()
