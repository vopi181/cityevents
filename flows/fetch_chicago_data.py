import httpx
import polars as pl
from prefect import flow, task
from datetime import datetime, timedelta

# https://data.cityofchicago.org/resource/nrmj-3kcf.json


@task(retries=2)
def get_chicago_data():
    """
    Fetches data from the City of Chicago regarding licenses
    """

    resp = httpx.get(
        "https://data.cityofchicago.org/resource/nrmj-3kcf.json?$limit=8000",
        headers={
            "Accept": "application/json",
            
        },
    )
    if resp.status_code != 200:
        raise Exception("Unable to fetch data from Chicago")
    df = pl.read_json(resp.content)
    return df


@flow(log_prints=True)
def chicago_data():
    data = get_chicago_data()

    # get license in the past 30 days
    recent_licenses = data.filter(
        pl.col("date_issued").str.to_datetime() > datetime.now() - timedelta(days=30)
    )

    print(f"There have been {recent_licenses['date_issued'].count()} licenses issued in the past 30 days")


if __name__ == "__main__":
    chicago_data()
