FROM apache/airflow:2.10.2

# Change user
USER airflow

# Instal packages
RUN pip install pandas seaborn matplotlib unidecode