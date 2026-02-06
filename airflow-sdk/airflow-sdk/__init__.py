from typing import Any

__all__ =["sql"]

def get_provider_info() -> dict[str, Any]:
    return{
        "package-name": "airflow-sdk",
        "name": "Airflow SDK",
        "description": "A custom SDK for Apache Airflow to simplify DAG and task creation.",
        "version": "0.0.1",
        "task-decorators": [
            {
                "name" : "sql",
                "class-name": "airflow-sdk.decorators.sql.sql_task",
        
            }
        ]
    }