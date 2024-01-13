from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/vopi181/cityevents",
        entrypoint="flows/fetch_chicago_data.py:chicago_data",
    ).deploy(
        name="chicago-data-deployment",
        work_pool_name="my-managed-pool",
        cron="0 1 * * *",
    )