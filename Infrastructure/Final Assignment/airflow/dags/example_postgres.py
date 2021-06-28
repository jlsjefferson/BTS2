import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
#from airflow.providers.mongo.hooks.mongo import MongoHook
#import airflow.providers.mongo.hooks

default_args = {"owner": "airflow"}

# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by
# instantiating the Postgres Operator

with DAG(
    dag_id="postgres_operator_dag",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
) as dag:
    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        postgres_conn_id="docker-postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS pet (
            pet_id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            OWNER VARCHAR NOT NULL);
          """,
    )
    populate_pet_table = PostgresOperator(
        task_id = "populate_pet_table",
        postgres_conn_id="docker-postgres",
        sql="""
            INSERT INTO pet VALUES ( 1,'Max', 'Dog', '2018-07-05', 'Jane');
            INSERT INTO pet VALUES ( 2,'Susie', 'Cat', '2019-05-01', 'Phil');
            INSERT INTO pet VALUES ( 3,'Lester', 'Hamster', '2020-06-23', 'Lily');
            INSERT INTO pet VALUES ( 4,'Quincy', 'Parrot', '2013-08-11', 'Anne');
            """,
        )
    get_all_pets = PostgresOperator(
        task_id="get_all_pets", postgres_conn_id="docker-postgres", sql="SELECT * FROM pet;"
    )
    get_birth_date = PostgresOperator(
         task_id="get_birth_date",
         postgres_conn_id="docker-postgres",
         sql="""
            SELECT * FROM pet
            WHERE birth_date
            BETWEEN SYMMETRIC {{ params.begin_date }} AND {{ params.end_date }};
            """,
         params={'begin_date': '"2020-01-01"', 'end_date': '"2020-12-31"'},
         )
    
    create_pet_table >> populate_pet_table >> get_all_pets >> get_birth_date

