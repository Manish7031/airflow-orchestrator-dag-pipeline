from airflow.sdk import asset, Asset, Context
import requests

@asset(
    name="user",
    schedule="@daily",
    uri="https://randomuser.me/api/"
)
def user(self) -> dict[any]:
    response = requests.get(self.uri)
    return response.json()

## create multi asset
@asset.multi(
    name="user_info",
    schedule="user",
    outlets=[
        Asset(name="user_loc"),
        Asset(name="user_login")
    ]
)
#create multi asset with multi outlet
def user_info(user: Asset, context: Context) -> list[dict[any]]:
    user_data = context['ti'].xcom_pull(
        dag_id=user.name,
        task_ids=user.name,
        include_prior_dates=True,
    )
    return [
        user_data['results'][0]['location'],
        user_data['results'][0]['login']
    ]

## create single asset with multi outlet
@asset(
    schedule="user"
)
def user_loc(user: Asset, context: Context) -> dict[any]:
    user_data = context['ti'].xcom_pull(
        dag_id=user.name,
        task_ids=user.name,
        include_prior_dates=True,
    )
    return user_data['results'][0]['location']
