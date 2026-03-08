from prefect import flow, task

from src.ingestion import scrape_earthquake_data
from src.load import load_to_s3
from src.transformation import transform_earthquake_data


@task(retries=3, retry_delay_seconds=60)
def scrape_task():
    return scrape_earthquake_data()


@task
def transform_task(data):
    return transform_earthquake_data(data)


@task(retries=2)
def load_task(data):
    load_to_s3(data)


@flow(name="PHIVOLCS-Earthquake-Pipeline")
def earthquake_flow():
    raw_data = scrape_task()
    clean_data = transform_task(raw_data)
    load_task(clean_data)


if __name__ == "__main__":
    earthquake_flow()
