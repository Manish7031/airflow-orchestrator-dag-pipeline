from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
from datetime import datetime


@dag
def user_processing_pipeline():
    create_table = SQLExecuteQueryOperator(
        task_id="create_user_table",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            firstname VARCHAR(100),
            lastname VARCHAR(100),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
     )

    @task.sensor(poke_interval=30, timeout=600)
    def is_api_available() -> PokeReturnValue:
        response = requests.get("./datasets/fakeuser.json")
        print(response.status_code)
        if response.status_code == 200:
            fake_user = response.json()
            return PokeReturnValue(is_done=True, xcom_value=fake_user)
            
        else:
            fake_user = None
            return PokeReturnValue(is_done=False, xcom_value=fake_user)
    
    @task
    def extract_user(fake_user):
        #fake_user = ti.xcom_pull(task_ids="is_api_available")
        # response = requests.get("./datasets/fakeuser.json")
        # fake_user = response.json()
        return {
            "id": fake_user["id"],
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"]
    }

    @task
    def process_user(user_info):
        import csv
        user_info["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open("/tmp/user_info.csv", "w", newline='') as F:
            writer = csv.DictWriter(F, fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)
        
    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id="postgres")
        hook.copy_expert(
            sql="COPY users from STDIN WITH CSV HEADER",
            filename="/tmp/user_info.csv"
        )

    ## set tasks dependencies  
    process_user(extract_user(create_table >> is_api_available())) >> store_user()
    # fake_user = is_api_available()
    # user_info = extract_user(fake_user)
    # process_user(user_info)
    # store_user()

user_processing_pipeline()

