from datetime import timedelta, datetime
import json
import os
import pandas as pd
from dags.etl_utils import create_sql_statements
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
import io

TABLES_CREATION_QUERY = """CREATE TABLE IF NOT EXISTS job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(225),
    industry VARCHAR(225),
    description TEXT,
    employment_type VARCHAR(125),
    date_posted DATE
);

CREATE TABLE IF NOT EXISTS company (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    name VARCHAR(225),
    link TEXT,
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS education (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    required_credential VARCHAR(225),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS experience (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    months_of_experience INTEGER,
    seniority_level VARCHAR(25),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS salary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    currency VARCHAR(3),
    min_value NUMERIC,
    max_value NUMERIC,
    unit VARCHAR(12),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS location (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    country VARCHAR(60),
    locality VARCHAR(60),
    region VARCHAR(60),
    postal_code VARCHAR(25),
    street_address VARCHAR(225),
    latitude NUMERIC,
    longitude NUMERIC,
    FOREIGN KEY (job_id) REFERENCES job(id)
)
"""

@task()
def extract(source_file, target_dir):
    """Extract data from jobs.csv."""
    df = pd.read_csv(source_file)
    context_data = df['context'].tolist()

    for idx, data in enumerate(context_data):
        if data:
            json_data = json.load(io.StringIO(data))
            output_file_path = f'{target_dir}/extracted_{idx}.json'
            with open(output_file_path, 'w') as file:
                json.dump(json_data, file, indent=2)
            print(f"File extracted: {output_file_path}")

@task()
def transform(extracted_dir, transformed_dir):
    """Clean and convert extracted elements to json."""
    for idx in range(100):  # assuming a maximum of 100 files
        input_file = f'{extracted_dir}/extracted_{idx}.json'
        if not os.path.exists(input_file):
            break

        with open(input_file, 'r') as file:
            data = json.load(file)

        # Transform data according to the desired schema
        transformed_data = {
            "job": {
                "title": data.get("title", ""),
                "industry": data.get("industry", ""),
                "description": data.get("description", ""),
                "employment_type": data.get("employmentType", ""),
                "date_posted": data.get("datePosted", ""),
            },
            "company": {
                "name": data.get("hiringOrganization", {}).get("name", ""),
                "link": data.get("hiringOrganization", {}).get("sameAs", ""),
            },
            "education": {
                "required_credential": data.get("educationRequirements", {}).get("credentialCategory", ""),
            },
            "experience": {
                "months_of_experience": data.get("experienceRequirements", {}).get("monthsOfExperience", ""),
                "seniority_level": "",
            },  # Seniority level not provided in the example
            "salary": {
                "currency": "",  # Extract currency information from data if available
                "min_value": "",  # Extract min value information from data if available
                "max_value": "",  # Extract max value information from data if available
                "unit": "",  # Extract unit information from data if available
            },
            "location": {
                "country": data.get("jobLocation", {}).get("address", {}).get("addressCountry", ""),
                "locality": data.get("jobLocation", {}).get("address", {}).get("addressLocality", ""),
                "region": data.get("jobLocation", {}).get("address", {}).get("addressRegion", ""),
                "postal_code": data.get("jobLocation", {}).get("address", {}).get("postalCode", ""),
                "street_address": data.get("jobLocation", {}).get("address", {}).get("streetAddress", ""),
                "latitude": data.get("jobLocation", {}).get("latitude", ""),
                "longitude": data.get("jobLocation", {}).get("longitude", ""),
            },
        }

        with open(f'{transformed_dir}/transformed_{idx}.json', 'w') as output_file:
            json.dump(transformed_data, output_file, indent=2)

@task()
def load(transformed_dir):
    """Load data to sqlite database."""
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')

    for idx in range(100):  # assuming a maximum of 100 files
        input_file = f'{transformed_dir}/transformed_{idx}.json'
        if not os.path.exists(input_file):
            break

        with open(input_file, 'r') as file:
            transformed_data = json.load(file)

        sql_statements = create_sql_statements(transformed_data)
        for statement in sql_statements:
            sqlite_hook.run(sql=statement)

DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():
    """ETL pipeline"""

    create_tables = SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY
    )

    source_file = '/mnt/c/Users/Admin/Documents/Data-Internship-Home-Assignment/source/jobs.csv'
    target_dir = '/mnt/c/Users/Admin/Documents/Data-Internship-Home-Assignment/staging/'

    extract_task = extract(source_file=source_file, target_dir=target_dir)
    transform_task = transform(extracted_dir=f'{target_dir}/extracted', transformed_dir=f'{target_dir}/transformed')
    load_task = load(transformed_dir=f'{target_dir}/transformed')

    create_tables >> extract_task >> transform_task >> load_task

etl_dag()
