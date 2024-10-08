import os
import pandas as pd
import logging
from datetime import datetime

def setup_logging():
    """
    Sets up the logging system.
    """
    # Logs directory inside the Docker container
    logs_dir = '/opt/airflow/logs'
    os.makedirs(logs_dir, exist_ok=True)

    # Log file name with the current date and time
    log_file_name = datetime.now().strftime('data_aggregation_%Y%m%d_%H%M%S.log')
    log_file_path = os.path.join(logs_dir, log_file_name)

    # Configures logging to save to a file and display in the console
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler(log_file_path, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

def get_latest_parquet_files(silver_dir):
    """
    Scans the Silver layer directory and returns a list of paths for the most recent Parquet files for each partition.

    Args:
        silver_dir (str): Path to the Silver layer directory.

    Returns:
        list: List of full paths to the most recent Parquet files.
    """
    latest_files = {}
    
    for root, dirs, files in os.walk(silver_dir):
        for file in files:
            if file.endswith('.parquet'):
                # Extract the timestamp from the file name
                # Assuming the file name follows the pattern 'data_YYYYMMDD_HHMMSS.parquet'
                try:
                    timestamp_str = file.replace('data_', '').replace('.parquet', '')
                    timestamp = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                except ValueError:
                    logging.warning(f'Invalid timestamp format in the file: {file}')
                    continue
                
                # Extract partitions from the file path
                relative_path = os.path.relpath(root, silver_dir)
                partitions = relative_path.split(os.sep)
                if len(partitions) < 2:
                    logging.warning(f'Unexpected partition structure for the file: {file}')
                    continue
                country, state = partitions[:2]
                
                partition_key = (country, state)
                
                # Check if there is already a file for this partition and update if the timestamp is more recent
                if partition_key not in latest_files or timestamp > latest_files[partition_key][1]:
                    latest_files[partition_key] = (os.path.join(root, file), timestamp)
    
    # Return only the file paths
    return [file_info[0] for file_info in latest_files.values()]

def create_gold_layer():
    """
    Reads the most recent files from the Silver Layer, performs aggregations by country/state/brewery type and by country/brewery type,
    and saves the results in the Gold Layer.
    """
    # Path to the Silver layer inside the Docker container
    silver_dir = '/opt/airflow/data/silver_layer'

    # Path to the Gold layer inside the Docker container
    gold_dir = '/opt/airflow/data/gold_layer'
    os.makedirs(gold_dir, exist_ok=True)

    # Get the most recent Parquet files
    latest_parquet_files = get_latest_parquet_files(silver_dir)
    if not latest_parquet_files:
        logging.error('No Parquet files found in the Silver Layer.')
        return

    logging.info(f'{len(latest_parquet_files)} most recent Parquet files found in the Silver Layer.')

    # Read and concatenate the selected Parquet files
    try:
        df_list = []
        for file_path in latest_parquet_files:
            df_part = pd.read_parquet(file_path, engine='pyarrow')
            df_list.append(df_part)
            logging.info(f'File read: {file_path} with {len(df_part)} records.')
        
        df = pd.concat(df_list, ignore_index=True)
        logging.info(f'Total records after concatenation: {len(df)}')
    except Exception as e:
        logging.error(f'Error reading the Parquet files: {e}')
        return

    # Add the 'snapshot_date' column
    snapshot_date = datetime.now().strftime('%Y-%m-%d')
    df['snapshot_date'] = snapshot_date

    # Add timestamp to the names of the aggregated files
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    ### Aggregation by country, state, and brewery type ###
    try:
        aggregation_state = df.groupby(['country', 'state', 'brewery_type'], observed=True).size().reset_index(name='brewery_count')
        aggregation_state['snapshot_date'] = snapshot_date
        state_file_name = f'brewery_aggregated_state_{timestamp}.parquet'
        state_file_path = os.path.join(gold_dir, state_file_name)
        aggregation_state.to_parquet(state_file_path, index=False)
        logging.info(f'Aggregation by country, state, and brewery type saved to {state_file_path}')
    except Exception as e:
        logging.error(f'Error during aggregation by country, state, and brewery type: {e}')
        return

    ### Aggregation by country and brewery type ###
    try:
        aggregation_country = df.groupby(['country', 'brewery_type'], observed=True).size().reset_index(name='brewery_count')
        aggregation_country['snapshot_date'] = snapshot_date
        country_file_name = f'brewery_aggregated_country_{timestamp}.parquet'
        country_file_path = os.path.join(gold_dir, country_file_name)
        aggregation_country.to_parquet(country_file_path, index=False)
        logging.info(f'Aggregation by country and brewery type saved to {country_file_path}')
    except Exception as e:
        logging.error(f'Error during aggregation by country and brewery type: {e}')
        return

def main():
    setup_logging()
    logging.info('Starting Gold Layer processing.')
    create_gold_layer()
    logging.info('Gold Layer processing completed.')

if __name__ == '__main__':
    main()