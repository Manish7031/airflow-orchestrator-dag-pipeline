from airflow.sdk import dag, task

@dag
def branch():
    @task
    def start_task():
        print("initiate task")
        return 1
        
    @task.branch
    def decide_task(val:int):
        if val == 1:
            return "branch1"
        return "branch2"

    @task
    def branch1(val:int):
        return f"branch1 execution with value {val}"

    @task
    def branch2(val:int):
        return f"branch2 execution with value {val}"
    
    val = start_task()
    decide_task(val) >> [branch1(val), branch2(val)]
 

branch()