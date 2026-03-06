from ingestion import scrape_earthquake_data
from load import load_to_s3
from transformation import transform_earthquake_data


def main():
    earthquake_data = scrape_earthquake_data()
    transformed_data = transform_earthquake_data(earthquake_data)
    load_to_s3(transformed_data)


if __name__ == "__main__":
    main()
