from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
from pathlib import Path
import tempfile
import csv, json

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
POSTGRES_CONN_ID = "docker-postgres"
MYSQL_CONN_ID = "docker-mysql"

def load_data_from_csv(**kwargs):
    """
    insert data from the local csv file
    """

    pg_hook = PostgresHook(postgres_conn_id = kwargs['conn_id'])
    pg_conn = pg_hook.get_conn()
    pg_cur = pg_conn.cursor()

    sql_statement = f"COPY {kwargs['table_name']} FROM STDIN WITH DELIMITER E'{kwargs['file_delimiter']}' CSV HEADER null as ';'"

    current_dir = AIRFLOW_HOME + "/dags/data_ori/"
    #print(f"The current_dir:{current_dir}")
    with open(current_dir + kwargs['file_name'], 'r') as f:
        pg_cur.copy_expert(sql_statement, f)
        pg_conn.commit() 
    
    pg_cur.close()
    pg_conn.close()


def export_postgresql_to_tmp_csv(**kwargs):
    """
    export table data from mysql to csv file
    """

    print(f"Entering export_postgresql_to_csv {kwargs['copy_sql']}")

    pg_hook = PostgresHook.get_hook(kwargs['conn_id'])

    current_dir = AIRFLOW_HOME + "/dags/data_exp/"

    with tempfile.NamedTemporaryFile(suffix=".csv", dir= current_dir) as temp_file:
        temp_name = temp_file.name        

        print(f"Exporting query to file {temp_name}")
        pg_hook.copy_expert(kwargs['copy_sql'], filename=temp_name)



def export_postgresql_to_v1_csv(**kwargs):
    """
    export table data from mysql to csv file
    """

    print(f"Entering export_postgresql_to_csv {kwargs['copy_sql']}")
    pg_hook = PostgresHook.get_hook(kwargs['conn_id'])

    current_dir = AIRFLOW_HOME + "/dags/data_exp/"
    exp_file_name = current_dir + kwargs['file_name']

    with open(exp_file_name, 'w'):
                pass

    pg_hook.copy_expert(sql = kwargs['copy_sql'], filename=exp_file_name)



def export_postgresql_to_csv(**kwargs):
    """
    export table data from postgresql to csv file
    """

    print(f"Entering export_postgresql_to_csv {kwargs['copy_sql']}")
    pg_hook = PostgresHook.get_hook(kwargs['conn_id'])

    current_dir = AIRFLOW_HOME + "/dags/data_exp/"
    exp_file_name = current_dir + kwargs['file_name']

    pg_hook.bulk_dump(table = kwargs['table_name'], tmp_file = exp_file_name)


def mysql_bulk_load_sql(**kwargs):
    current_dir = AIRFLOW_HOME + "/dags/data_exp/"
    exp_file_name = current_dir + kwargs['file_name']

    conn = MySqlHook(mysql_conn_id=kwargs['conn_id'])
    conn.bulk_load(table = kwargs['table_name'], tmp_file = exp_file_name)


def export_mysql_to_csv_v1(**kwargs):
    """
    export table data from mysql to csv file
    """

    print(f"Entering export_mysql_to_csv {kwargs['conn_id']}")
    mysql_hook = MySqlHook(mysql_conn_id=kwargs['conn_id'])

    current_dir = AIRFLOW_HOME + "/dags/data_exp/"
    exp_file_name = current_dir + kwargs['file_name']

    mysql_hook.bulk_dump(table = kwargs['table_name'], tmp_file = exp_file_name)


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
    #mongo_hook = MongoHook(mongo_conn_id=kwargs['conn_id'])
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
    'postgretomysql',
    default_args=default_args,
    start_date=days_ago(2),
    tags=['etl'],
)

# [Create Postgresql tables if not exists]

recreate_postgres_schema = PostgresOperator(
    task_id="create_postgresql_schema",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="/sql/postgresql-test-ddl-tables.sql",
    dag=dag,
    )

# [Load and transform Postgresql tables]

load_transform_customer = PythonOperator(
    task_id='load_transform_customer',
    python_callable=load_data_from_csv,
    op_kwargs={'conn_id': POSTGRES_CONN_ID, 'file_name': 'customer.tsv', 'table_name': 'test.customer', 'file_delimiter': '\t'},
    provide_context=True,
    dag=dag)

load_transform_invoice = PythonOperator(
    task_id='load_transform_invoice',
    python_callable=load_data_from_csv,
    op_kwargs={'conn_id': POSTGRES_CONN_ID, 'file_name': 'invoice.tsv', 'table_name': 'test.invoice', 'file_delimiter': '\t'},
    provide_context=True,
    dag=dag)

# [Export Postgresql tables to tsv]

export_postgresql_customer = PythonOperator(
    task_id='export_postgresql_customer',
    python_callable=export_postgresql_to_csv,
    op_kwargs={'conn_id': POSTGRES_CONN_ID, 'file_name': 'customer.tsv', 'table_name': 'test.customer', 'copy_sql': 'SELECT * FROM test.customer', 'file_delimiter': '\t'},
    provide_context=True,
    dag=dag)

export_postgresql_invoice = PythonOperator(
    task_id='export_postgresql_invoice',
    python_callable=export_postgresql_to_csv,
    op_kwargs={'conn_id': POSTGRES_CONN_ID, 'file_name': 'invoice.tsv', 'table_name': 'test.invoice', 'copy_sql': 'SELECT * FROM test.invoice', 'file_delimiter': '\t'},
    provide_context=True,
    dag=dag)

# [Create Mysql tables if not exists]

recreate_mysql_schema = MySqlOperator(
    task_id="recreate_mysql_schema",
    mysql_conn_id=MYSQL_CONN_ID,
    sql="/sql/mysql-test-ddl-tables_exp.sql",
    dag=dag,
    )

# [Load and transform Mysql tables]

import_mysql_customer = PythonOperator(
    task_id='import_mysql_customer',
    python_callable=mysql_bulk_load_sql,
    op_kwargs={'conn_id': MYSQL_CONN_ID, 'file_name': 'customer.tsv', 'table_name': 'test.customer', 'conn_schema': 'test', 'file_delimiter': '\t'},
    provide_context=True,
    dag=dag)

import_mysql_invoice = PythonOperator(
    task_id='import_mysql_invoice',
    python_callable=mysql_bulk_load_sql,
    op_kwargs={'conn_id': MYSQL_CONN_ID, 'file_name': 'invoice.tsv', 'table_name': 'test.invoice', 'conn_schema': 'test', 'file_delimiter': '\t'},
    provide_context=True,
    dag=dag)


recreate_postgres_schema >> load_transform_customer >> load_transform_invoice >> export_postgresql_customer >> export_postgresql_invoice >> recreate_mysql_schema >> import_mysql_customer >> import_mysql_invoice
