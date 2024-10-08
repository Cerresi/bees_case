import os
import pandas as pd
import logging
from datetime import datetime
import json
import time
from helpers import load_replacements
import unidecode

def setup_logging():
    """
    Sets up the logging system.
    """
    # Logs directory inside the Docker container
    logs_dir = '/opt/airflow/logs'

    # Ensure the logs directory exists
    os.makedirs(logs_dir, exist_ok=True)

    # Log file name with timestamp for unique identification
    log_file_name = datetime.now().strftime('data_transformation_%Y%m%d_%H%M%S.log')
    log_file_path = os.path.join(logs_dir, log_file_name)

    # Logging configuration for file and console output
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler(log_file_path, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

def clean_partition_value(value):
    """
    Cleans the value used for partitioning by removing or replacing special characters,
    converting to lowercase, and ensuring ASCII compliance.
    """
    if not isinstance(value, str):
        value = str(value)

    # Remove leading and trailing whitespace
    value = value.strip()

    # Convert to lowercase
    value = value.lower()

    # Replace multiple spaces with a single space
    value = ' '.join(value.split())

    # Convert accented characters to ASCII equivalents
    value = unidecode.unidecode(value)

    # Replace spaces with underscores to ensure valid partition directories
    value = value.replace(' ', '_')

    # Remove any non-alphanumeric characters except underscores
    value = ''.join(char for char in value if char.isalnum() or char == '_')

    return value

def save_partitioned_parquet(df, silver_dir, timestamp):
    """
    Saves the DataFrame partitioned by 'country' and 'state', with Parquet files named using a timestamp.
    
    Args:
        df (pd.DataFrame): DataFrame to be saved.
        silver_dir (str): Base directory for saving the Parquet files.
        timestamp (str): Timestamp to include in the file names.
    """
    # Group the DataFrame by the partition columns
    grouped = df.groupby(['country', 'state'])

    for (country, state), group in grouped:
        # Define the partition directory
        partition_dir = os.path.join(silver_dir, country, state)
        os.makedirs(partition_dir, exist_ok=True)

        # Define the Parquet file name with the timestamp
        parquet_file_name = f'data_{timestamp}.parquet'
        parquet_file_path = os.path.join(partition_dir, parquet_file_name)

        try:
            # Save the group as a Parquet file
            group.to_parquet(
                path=parquet_file_path,
                engine='pyarrow',
                index=False
            )
            logging.info(f'Data saved to {parquet_file_path}')
        except Exception as e:
            logging.error(f'Error saving data to {parquet_file_path}: {e}')

def transform_breweries():
    """
    Transforms raw brewery data and saves it to Parquet files partitioned by country and state.
    This process includes data cleaning, applying replacements for corrupted characters,
    and adding a 'snapshot_date' column for versioning purposes.
    """
    # Path to the bronze layer inside the Docker container
    bronze_dir = '/opt/airflow/data/bronze_layer'

    # Find the most recent raw data file in the bronze layer
    files_in_bronze = [f for f in os.listdir(bronze_dir) if f.startswith('breweries_raw_') and f.endswith('.json')]
    if not files_in_bronze:
        logging.error('No file found in the bronze layer.')
        return

    # Sort files by date (descending) and select the most recent
    files_in_bronze.sort(reverse=True)
    bronze_file = os.path.join(bronze_dir, files_in_bronze[0])

    # Path to the silver layer inside the Docker container
    silver_dir = '/opt/airflow/data/silver_layer'

    # Create the silver layer directory if it doesn't exist
    os.makedirs(silver_dir, exist_ok=True)

    # Create a timestamp for the Parquet file names
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Load raw data from the most recent JSON file
    try:
        with open(bronze_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        logging.info(f'Data successfully loaded from {bronze_file}')
    except Exception as e:
        logging.error(f'Error reading the JSON file: {e}')
        return

    # Filter out invalid brewery types
    df = df[df['brewery_type'] != 'location']
    logging.info(f'Invalid brewery types removed. Remaining records: {len(df)}')

    # Load the replacement dictionary from the helpers file
    replacements = load_replacements()

    def replace_text(text):
        """
        Replaces corrupted text using the replacement dictionary.
        """
        return replacements.get(text, text)

    # List of text columns where replacements need to be applied
    text_columns = ['name', 'brewery_type', 'address_1', 'address_2', 'address_3', 'city', 'state_province', 'state', 'country', 'street']
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].apply(replace_text)
        else:
            logging.warning(f'Column "{col}" not found in the DataFrame.')

    # Clean and standardize 'country' and 'state' columns for partitioning
    if 'country' in df.columns and 'state' in df.columns:
        df['country'] = df['country'].apply(clean_partition_value)
        df['state'] = df['state'].apply(clean_partition_value)
    else:
        logging.error('Columns "country" and/or "state" not found in the DataFrame.')
        return

    # Add 'snapshot_date' column with the full timestamp
    snapshot_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['snapshot_date'] = snapshot_date

    # Save the transformed data as Parquet files with timestamped names
    save_partitioned_parquet(df, silver_dir, timestamp)

def main():
    """
    Main function to set up logging, initiate the data transformation process,
    and log the execution time. This ensures that all steps are logged and performance metrics are captured for monitoring.
    """
    setup_logging()
    logging.info('Starting data transformation process.')

    # Capture start time to measure total execution time
    start_time = time.time()
    transform_breweries()
    end_time = time.time()

    # Calculate and log the total execution duration
    duration = end_time - start_time
    logging.info(f'Data transformation process completed in {duration:.2f} seconds.')

if __name__ == '__main__':
    main()