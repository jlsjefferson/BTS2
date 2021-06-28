from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

MONGO_CONN_ID = "docker-mongo"

def mysql_bulk_load_mongo(**kwargs):

    data={ "id": 1, "name": "Roberto", "country": "Mexico" }
    #dict = { "name": "Kiku", "address": "Germany" }
    
    conn = MongoHook(conn_id=kwargs['conn_id'])
    conn.insert_one(mongo_collection = kwargs['table_name'], doc=data, mongo_db='test')
    
default_args = {
    'owner': 'airflow',
}

dag = DAG(
    'example_mongo',
    default_args=default_args,
    start_date=days_ago(2),
    tags=['mongodb'],
)

# [START howto_operator_mysql_external_file]

import_mysql_customer = PythonOperator(
    task_id='import_mysql_customer',
    python_callable=mysql_bulk_load_mongo,
    op_kwargs={'conn_id': MONGO_CONN_ID, 'file_name': 'customer.tsv', 'table_name': 'customer', 'conn_schema': 'test', 'file_delimiter': '\t'},
    provide_context=True,
    dag=dag)

# [END howto_operator_mysql_external_file]

#drop_table_mysql_task >> mysql_task