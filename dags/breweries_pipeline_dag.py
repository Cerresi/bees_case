from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define the DAG
default_args = {
    'owner': 'Lucas',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['cerresi@gmail.com'],
    'email_on_failure': True,  
    'email_on_retry': True,  
    'retries': 1,  
    'retry_delay': timedelta(seconds=10), 
}

# Define the DAG
with DAG('breweries_data_pipeline',
         description='Pipeline for ingestion and transformation of brewery data',
         schedule_interval='@daily',
         start_date=days_ago(1),
         catchup=False) as dag:

    # Ingestion task
    ingest_bronze = BashOperator(
        task_id='data_ingestion',
        bash_command='/usr/local/bin/python3 /opt/airflow/scripts/data_ingestion.py',
        dag=dag
    )

    # Transformation task for silver layer
    transform_silver = BashOperator(
        task_id='data_transformation',
        bash_command='/usr/local/bin/python3 /opt/airflow/scripts/data_transformation.py'
    )

    # Aggregation task for gold layer
    aggregate_gold = BashOperator(
        task_id='data_aggregation',
        bash_command='/usr/local/bin/python3 /opt/airflow/scripts/data_aggregation.py'
    )

    # Aggregation task for the report script
    analyse_report = BashOperator(
        task_id='data_analyse',
        bash_command='/usr/local/bin/python3 /opt/airflow/scripts/data_analyse.py'
    )

    # Define task order
    ingest_bronze >> transform_silver >> aggregate_gold >> analyse_report