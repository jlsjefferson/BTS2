from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
#from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
#from airflow.providers.mongo.hooks.mongo import MongoHook
#from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
from pathlib import Path
import tempfile

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
MYSQL_CONN_ID = "docker-mysql"
MONGO_CONN_ID = "docker-mongo"

def mysql_bulk_load_mongo(**kwargs):
    current_dir = AIRFLOW_HOME + "/dags/data_exp/"
    exp_file_name = current_dir + kwargs['file_name']

    #conn = MySqlHook(default_conn_name = kwargs['conn_id'], conn_name_attr=kwargs['conn_id'], schema=kwargs['conn_schema'])
    #conn = MySqlHook(default_conn_name = kwargs['conn_id'], schema=kwargs['conn_schema'])
    conn = MongoHook(mysql_conn_id=kwargs['conn_id'])
    #conn.aggregate(mongo_collection = kwargs['table_name'], tmp_file = exp_file_name)
    #conn.insert_many(mongo_collection = kwargs['table_name'], tmp_file = exp_file_name)
    conn.insert_one(mongo_collection = kwargs['table_name'], mongo_db='test')
    
    
# [Defining args]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 31),
    "email": ["tech@innospark.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# [Defining Dag]

dag = DAG(
    'mysqltomongo',
    default_args=default_args,
    start_date=days_ago(2),
    tags=['mongo'],
)

import_mysql_customer = PythonOperator(
    task_id='import_mysql_customer',
    python_callable=mysql_bulk_load_mongo,
    op_kwargs={'conn_id': MONGO_CONN_ID, 'file_name': 'customer.tsv', 'table_name': 'test.customer', 'conn_schema': 'test', 'file_delimiter': '\t'},
    provide_context=True,
    dag=dag)
