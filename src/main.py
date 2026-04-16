from prefect import flow, task, get_run_logger

from src.ingestion import scrape_earthquake_data
from src.load import load_to_s3
from src.transformation import transform_earthquake_data


@task(retries=3, retry_delay_seconds=60)
def scrape_task():
    logger = get_run_logger()
    logger.info("Starting earthquake data scraping from PHIVOLCS...")
    data = scrape_earthquake_data()
    logger.info(f"Scraping complete. Fetched {len(data)} rows.")
    return data


@task
def transform_task(data):
    logger = get_run_logger()
    logger.info(f"Starting transformation on {len(data)} rows...")
    result = transform_earthquake_data(data)
    logger.info("Transformation complete.")
    return result


@task(retries=2)
def load_task(data):
    logger = get_run_logger()
    logger.info(f"Loading {len(data)} rows to S3...")
    load_to_s3(data)
    logger.info("Load to S3 complete.")


@flow(name="PHIVOLCS-Earthquake-Pipeline")
def earthquake_flow():
    logger = get_run_logger()
    logger.info("PHIVOLCS Earthquake Pipeline started.")

    raw_data = scrape_task()
    clean_data = transform_task(raw_data)
    load_task(clean_data)

    logger.info("PHIVOLCS Earthquake Pipeline finished successfully.")


if __name__ == "__main__":
    earthquake_flow()