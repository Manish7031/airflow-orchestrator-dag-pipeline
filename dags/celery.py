from time import sleep
from airflow.sdk import dag, task
from airflow.providers.celery.operators.celery import CeleryOperator
from datetime import datetime

@dag
def celery_dag():

    @task
    def task1():
        sleep(5)

    @task(
        queue="high_cpu"
    )
    def task2():
        sleep(5)
    
    @task(
        queue="high_memory"
    )
    def task3():
        sleep(5)
    
    @task
    def task4():
        sleep(5)
    
    task1 >> [task2(), task3()] >> task4()

celery_dag()