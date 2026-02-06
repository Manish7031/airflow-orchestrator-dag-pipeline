from airflow.sdk import dag, task, Context

@dag
def xcom_dag():
    @task
    def push_xcom(context: Context):
        val = 1001
        context.xcom_push(key="my_key", value=val)
        # return val
    
    @task
    def pull_xcom(context: Context):
        xcom_value = context.xcom_pull(key="my_key", task_ids="push_xcom")
        print(f"Pulled XCom value: {xcom_value}")
    
    push_xcom() >> pull_xcom()
    # xcom_value = push_xcom()
    # pull_xcom(xcom_value)

xcom_dag()  