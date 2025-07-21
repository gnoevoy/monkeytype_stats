import pendulum
from airflow.sdk import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def monkeytype_pipeline():

    @task()
    def counter():
        for i in range(5):
            print(f"Counter: {i}")

    first_task = counter()


monkeytype_pipeline()
