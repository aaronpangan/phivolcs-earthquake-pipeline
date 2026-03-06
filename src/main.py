from ingestion import scrape_earthquake_data
from transformation import transform_earthquake_data


def main():
    earthquake_data = scrape_earthquake_data()
    transformed_data = transform_earthquake_data(earthquake_data)
    print(transformed_data.head())

if __name__ == "__main__":
    main()
