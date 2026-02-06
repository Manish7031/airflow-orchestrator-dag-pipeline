from airflow.sdk import dag, task, task_group

@dag
def group_dag():

    @task
    def task1():
        print("Task 1")
        return True
    
    @task_group(default_args={"retries": 3})
    def my_group(val: bool):

        @task
        def task2(myVal: bool):
            print("Task 2")
            return myVal
        
        @task_group
        def nested_group():

            @task
            def task3():
                print("Task 3")

            @task
            def task4():
                print("Task 4")
            
            task3() >> task4()
        task2(val) >> nested_group()
    task1() >> my_group()

group_dag()