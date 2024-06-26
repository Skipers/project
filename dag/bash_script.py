import airflow 
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from functions.func_xcom_push import run_spark_and_push_to_xcom
from functions.func_xcom_pull import load_to_postgresql
import os
from dotenv import load_dotenv

load_dotenv()

connect_to_postgres = os.environ.get('postgres_connect_db')


spark_args = { 
'owner': 'airflow', 
'start_date': datetime(2024,5,17),
'depends_on_past': False, 
'email': ['airflow@example.com'], 
'email_on_failure': False, 
'email_on_retry': False}


dag= DAG( 
dag_id = "project", 
default_args = spark_args, 
schedule_interval='0 * * * *', 
dagrun_timeout=timedelta(minutes=60), 
start_date = airflow.utils.dates.days_ago(1))


sql_drop_table = PostgresOperator(
	task_id = 'drop_table',
	postgres_conn_id = connect_to_postgres,
	sql = """ DROP TABLE IF EXISTS events;""",
	dag = dag)


sql_create_table = PostgresOperator(
    task_id = 'sql_command',
    postgres_conn_id = connect_to_postgres,
	sql = """CREATE TABLE IF NOT EXISTS events (
       user_id VARCHAR ,
       product_identifier VARCHAR ,
       start_time TIMESTAMP,
       end_time TIMESTAMP,
	   price_in_usd FLOAT);""",
	   dag=dag)


save_path_to_xcom = PythonOperator(
    task_id='save_path_to_xcom',
    python_callable = run_spark_and_push_to_xcom,
    provide_context=True,
    dag=dag)


load_data_sql = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_to_postgresql,
    provide_context=True,
    dag=dag)
   
sql_drop_table >> save_path_to_xcom >> sql_create_table  >> load_data_sql


if __name__ == "__main__": 
	dag.cli()