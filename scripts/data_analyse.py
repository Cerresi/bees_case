import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import glob
import logging
from datetime import datetime

# Define constants
LOGS_DIR = '/opt/airflow/logs'
GOLD_LAYER_DIR = '/opt/airflow/data/gold_layer'
REPORTS_DIR = '/opt/airflow/reports'
TIMESTAMP_FORMAT = '%Y%m%d_%H%M%S'

def setup_logging():
    """
    Sets up the logging system.
    """
    # Ensure the logs directory exists
    os.makedirs(LOGS_DIR, exist_ok=True)

    # Generate the log file name with the current date and time
    log_file_name = datetime.now().strftime(f'data_visualization_{TIMESTAMP_FORMAT}.log')
    log_file_path = os.path.join(LOGS_DIR, log_file_name)

    # Configure logging to save to a file and display in the console
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler(log_file_path, encoding='utf-8'),   
            logging.StreamHandler()                                
        ]
    )

def get_latest_file(directory, prefix):
    """
    Retrieves the latest file from the given directory based on the file prefix and timestamp in the filename.

    Args:
        directory (str): Directory path to search.
        prefix (str): Prefix of the file name.

    Returns:
        str: Path to the latest file, or None if no file is found.
    """
    # Find all files in the directory matching the prefix pattern
    list_of_files = glob.glob(os.path.join(directory, f'{prefix}*.parquet'))
    if not list_of_files:
        logging.error(f"No file found with the prefix: {prefix}")
        return None
    # Select the most recently modified file
    latest_file = max(list_of_files, key=os.path.getctime)
    logging.info(f'Latest file found: {latest_file}')
    return latest_file

def add_value_labels(ax, spacing=5):
    """
    Adds value labels on top of the bars in a bar chart.

    Args:
        ax (matplotlib.axes._axes.Axes): Matplotlib axis object.
        spacing (int, optional): Space between the label and the bar.
    """
    # Iterate over all bars in the chart
    for rect in ax.patches:
        # Get the height and position of the bar
        y_value = rect.get_height()
        x_value = rect.get_x() + rect.get_width() / 2
        # Determine the vertical alignment based on the value
        va = 'bottom' if y_value >= 0 else 'top'
        # Format the label text
        label = f"{int(y_value)}"
        # Place the annotation on the bar
        ax.annotate(
            label,
            (x_value, y_value),
            xytext=(0, spacing),  # Offset label by spacing
            textcoords="offset points",
            ha='center',
            va=va
        )

def create_pie_chart_with_others(dataframe, location, location_column, save_path, group_by=None, threshold=2.5):
    """
    Creates an ordered pie chart with an "Others" category for the distribution of brewery types in a specific location.

    Args:
        dataframe (pd.DataFrame): DataFrame containing the brewery data.
        location (str): Location (country or state) to filter the data.
        location_column (str): Column to filter the location.
        save_path (str): Path to save the chart.
        group_by (list, optional): Columns to group the data. Defaults to None.
        threshold (float): Minimum percentage to display a category separately. Smaller percentages are grouped into 'Others'.
    """
    # Group the data if grouping columns are provided
    if group_by:
        dataframe = dataframe.groupby(group_by).sum().reset_index()

    # Filter the DataFrame for the specified location
    df_location = dataframe[dataframe[location_column] == location]
    df_location = df_location.sort_values('brewery_count', ascending=False)

    # Calculate the percentage for each brewery type
    total_breweries = df_location['brewery_count'].sum()
    df_location['percentage'] = (df_location['brewery_count'] / total_breweries) * 100

    # Identify categories below the threshold to group into 'Others'
    df_others = df_location[df_location['percentage'] < threshold]
    if not df_others.empty:
        others_count = df_others['brewery_count'].sum()
        # Exclude the small categories from the main DataFrame
        df_location = df_location[df_location['percentage'] >= threshold]
        # Append the 'Others' category
        df_location = pd.concat([
            df_location,
            pd.DataFrame({
                'brewery_type': ['others'],
                'brewery_count': [others_count],
                'percentage': [(others_count / total_breweries) * 100]
            })
        ])

    # Create the pie chart
    plt.figure(figsize=(8, 8))
    plt.pie(
        df_location['brewery_count'],
        labels=df_location['brewery_type'],
        autopct='%1.1f%%',
        startangle=140
    )
    plt.title(f'Distribution of Brewery Types in {location.title().replace("_", " ")}', pad=20)
    plt.axis('equal')  # Ensure the pie chart is a circle

    # Save the pie chart to the specified path
    try:
        plt.savefig(save_path, dpi=300)
        logging.info(f'Pie chart saved: {save_path}')
    except Exception as e:
        logging.error(f'Error saving pie chart: {e}')

    plt.show()

def main():
        """
        Main function to set up logging, initiate the data visualization process,
        and generate charts based on the latest data from the Gold Layer.
        """
        setup_logging()
        logging.info('Starting data visualization process.')

        # Ensure the reports directory exists
        os.makedirs(REPORTS_DIR, exist_ok=True)
        logging.info(f'Reports directory verified: {REPORTS_DIR}')

        # Get the current timestamp for naming files
        timestamp = datetime.now().strftime(TIMESTAMP_FORMAT)

        # Retrieve the most recent aggregated files from the Gold Layer
        latest_country_file = get_latest_file(GOLD_LAYER_DIR, 'brewery_aggregated_country_')
        latest_state_file = get_latest_file(GOLD_LAYER_DIR, 'brewery_aggregated_state_')

        if latest_country_file and latest_state_file:
            try:
                # Load the data into DataFrames
                df_country = pd.read_parquet(latest_country_file)
                df_state = pd.read_parquet(latest_state_file)
                logging.info(f'{len(df_country)} records read from the country file.')
                logging.info(f'{len(df_state)} records read from the state file.')
            except Exception as e:
                logging.error(f'Error reading Parquet files: {e}')
                return

            # Bar Chart for Top 10 US States by brewery count
            us_state_totals = df_state[df_state['country'] == 'united_states'].groupby('state')['brewery_count'].sum().reset_index()
            us_state_totals = us_state_totals.sort_values(by='brewery_count', ascending=False).head(10)

            plt.figure(figsize=(12, 6))
            ax = sns.barplot(data=us_state_totals, x='state', y='brewery_count', hue='state', dodge=False)

            plt.title('Top 10 Brewery-Producing States in the United States')
            plt.xlabel('State')
            plt.ylabel('Number of Breweries')
            plt.xticks(rotation=45)

            # Add labels with the numbers on top of the bars
            add_value_labels(ax)

            # Save the bar chart with timestamp
            bar_chart_file = os.path.join(REPORTS_DIR, f'top_10_breweries_states_{timestamp}.png')
            try:
                plt.savefig(bar_chart_file, dpi=300)
                logging.info(f'Bar chart saved: {bar_chart_file}')
            except Exception as e:
                logging.error(f'Error saving bar chart: {e}')
            plt.show()

            # Bar Chart for Top 10 Countries by brewery count
            top_10_countries = df_country.groupby('country')['brewery_count'].sum().reset_index()
            top_10_countries = top_10_countries.sort_values(by='brewery_count', ascending=False).head(10)

            plt.figure(figsize=(12, 6))
            ax = sns.barplot(data=top_10_countries, x='country', y='brewery_count', hue='country', dodge=False)

            plt.title('Top 10 Brewery-Producing Countries')
            plt.xlabel('Country')
            plt.ylabel('Number of Breweries')
            plt.xticks(rotation=45)

            # Add labels with the numbers on top of the bars
            add_value_labels(ax)

            # Save the bar chart with timestamp
            bar_chart_country_file = os.path.join(REPORTS_DIR, f'top_10_breweries_countries_{timestamp}.png')
            try:
                plt.savefig(bar_chart_country_file, dpi=300)
                logging.info(f'Bar chart saved: {bar_chart_country_file}')
            except Exception as e:
                logging.error(f'Error saving bar chart: {e}')
            plt.show()

            # Pie Chart for United States with "Others" category
            pie_chart_us_file = os.path.join(REPORTS_DIR, f'brewery_distribution_united_states_ordered_{timestamp}.png')
            create_pie_chart_with_others(
                dataframe=df_state,
                location='united_states',
                location_column='country',
                save_path=pie_chart_us_file,
                group_by=['country', 'brewery_type'],
                threshold=2.5
            )

        else:
            logging.error("One or both of the most recent files were not found.")

        logging.info('Data visualization process completed.')

if __name__ == '__main__':
    main()
