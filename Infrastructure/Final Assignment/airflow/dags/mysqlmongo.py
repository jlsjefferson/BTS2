from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
from pathlib import Path
import tempfile
import csv, json

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
MYSQL_CONN_ID = "docker-mysql"
MONGO_CONN_ID = "docker-mongo"


def export_mysql_to_csv(**kwargs):
    """
    export table data from mysql to csv file
    """

    print(f"Entering export_mysql_to_csv {kwargs['conn_id']}")
    mysql_hook = MySqlHook(mysql_conn_id=kwargs['conn_id'])

    current_dir = AIRFLOW_HOME + "/dags/data_exp/"
    exp_file_name = current_dir + kwargs['file_name']

    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(kwargs['copy_sql'])

    with open(exp_file_name, "w", newline='') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter='\t')
        csv_writer.writerow([i[0] for i in cursor.description]) # write headers
        csv_writer.writerows(cursor)  


def import_mongo_from_csv(**kwargs):
    """
    import mongo from csv file
    """

    #http://airflow.apache.org/docs/apache-airflow-providers-mongo/stable/_api/airflow/providers/mongo/hooks/mongo/index.html#module-airflow.providers.mongo.hooks.mongo

    print(f"Entering import_mongo_from_csv {kwargs['conn_id']}")
    mongo_hook = MongoHook(conn_id = kwargs['conn_id'])

    current_dir = AIRFLOW_HOME + "/dags/data_exp/"
    exp_file_name = current_dir + kwargs['file_name']

    #TSV to Dictionary
    data = {}
    data_array = []
    with open(exp_file_name, newline='') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter='\t')
        for rows in csv_reader:
            print(f"the row {rows}")
            data_array.append(dict(rows))

    #data = 
    print(f"The data {data_array}")
    print(f"The data type {type(data_array)}")
    conn = mongo_hook.get_conn()
    print(f"The conn {conn}")

    #mongo_hook.insert_many(mongo_collection = kwargs['collection'], docs = [doc for doc in data_array], mongo_db = kwargs['database'])
    #Delete
    filter_doc = { "id" : { "$gt" : "0" } }
    mongo_hook.delete_many(mongo_collection = kwargs['collection'], filter_doc = filter_doc, mongo_db = kwargs['database'])
    #Insert
    mongo_hook.insert_many(mongo_collection = kwargs['collection'], docs = data_array, mongo_db = kwargs['database'])



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
    tags=['etl'],
)

# [Export Mysql tables to tsv]

export_mysql_customer = PythonOperator(
    task_id='export_mysql_customer',
    python_callable=export_mysql_to_csv,
    op_kwargs={'conn_id': MYSQL_CONN_ID, 'file_name': 'customer_mysql.tsv', 'table_name': 'test.customer', 'copy_sql': 'SELECT * FROM test.customer', 'file_delimiter': '\t'},
    provide_context=True,
    dag=dag)

export_mysql_invoice = PythonOperator(
    task_id='export_mysql_invoice',
    python_callable=export_mysql_to_csv,
    op_kwargs={'conn_id': MYSQL_CONN_ID, 'file_name': 'invoice_mysql.tsv', 'database': 'test', 'collection': 'test_', 'copy_sql': 'SELECT * FROM test.invoice', 'file_delimiter': '\t'},
    provide_context=True,
    dag=dag)

# [Import from tsv to Mongodb]

import_mongo_customer = PythonOperator(
    task_id='import_mongo_customer',
    python_callable=import_mongo_from_csv,
    op_kwargs={'conn_id': MONGO_CONN_ID, 'file_name': 'customer_mysql.tsv', 'database': 'test', 'collection': 'customer', 'file_delimiter': '\t'},
    provide_context=True,
    dag=dag)

import_mongo_invoice = PythonOperator(
    task_id='import_mongo_invoice',
    python_callable=import_mongo_from_csv,
    op_kwargs={'conn_id': MONGO_CONN_ID, 'file_name': 'invoice_mysql.tsv', 'database': 'test', 'collection': 'invoice', 'file_delimiter': '\t'},
    provide_context=True,
    dag=dag)

export_mysql_customer >> export_mysql_invoice >> import_mongo_customer >> import_mongo_invoice
