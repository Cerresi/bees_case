import os
import requests
import json
import logging
import time
from datetime import datetime

# Define constants
TIMESTAMP_FORMAT = '%Y%m%d_%H%M%S'
LOGS_DIR = os.environ.get('LOGS_DIR', '/opt/airflow/logs')
BRONZE_LAYER_DIR = os.environ.get('BRONZE_LAYER_DIR', '/opt/airflow/data/bronze_layer')

def setup_logging():
    """
    Sets up the logging system.
    """
    os.makedirs(LOGS_DIR, exist_ok=True)
    log_file_name = datetime.now().strftime(f'data_ingestion_{TIMESTAMP_FORMAT}.log')
    log_file_path = os.path.join(LOGS_DIR, log_file_name)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file_path, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

def fetch_breweries(per_page=50, max_retries=3):
    """
    Fetches data from the Open Brewery DB API and saves it to the bronze layer.
    Uses pagination to retrieve data in chunks, with a default of 50 breweries per page.
    Implements retry logic to handle timeout and connection errors.
    """
    breweries = []
    page = 1

    while True:
        url = f'https://api.openbrewerydb.org/breweries?page={page}&per_page={per_page}'
        retries = 0

        while retries < max_retries:
            try:
                response = requests.get(url, timeout=5)
                response.raise_for_status()
                data = response.json()
                break
            except requests.exceptions.RequestException as e:
                retries += 1
                logging.error(f'Error occurred for page {page}: {e}. Retrying {retries}/{max_retries}...')
                time.sleep(2)
        else:
            logging.error(f'Max retries reached for page {page}, skipping...')
            page += 1
            continue

        if not data:
            logging.info('All data has been retrieved.')
            break

        breweries.extend(data)
        logging.info(f'Page {page} retrieved with {len(data)} records.')
        page += 1

    logging.info(f'Total records retrieved: {len(breweries)}')
    save_to_bronze_layer(breweries)

def save_to_bronze_layer(breweries):
    """
    Saves the retrieved brewery data to the bronze layer in JSON format.
    The bronze layer is used to store raw data.
    """
    os.makedirs(BRONZE_LAYER_DIR, exist_ok=True)
    snapshot_timestamp = datetime.now().strftime(TIMESTAMP_FORMAT)
    bronze_file_name = f'breweries_raw_{snapshot_timestamp}.json'
    bronze_file_path = os.path.join(BRONZE_LAYER_DIR, bronze_file_name)

    with open(bronze_file_path, 'w', encoding='utf-8') as f:
        json.dump(breweries, f, ensure_ascii=False, indent=4)

    logging.info(f'Data saved to bronze layer at {bronze_file_path}')

def main():
    """
    Main function to set up logging and initiate the data ingestion process.
    """
    setup_logging()
    logging.info('Starting data ingestion process.')

    start_time = time.time()
    fetch_breweries()
    end_time = time.time()

    duration = end_time - start_time
    logging.info(f'Data ingestion process completed successfully in {duration:.2f} seconds.')

if __name__ == '__main__':
    main()