from datetime import timedelta, datetime
import json
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import io
import re
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


TABLES_CREATION_QUERY = """
CREATE TABLE IF NOT EXISTS job (
    title VARCHAR(225),
    industry VARCHAR(225),
    description TEXT,
    employment_type VARCHAR(125),
    date_posted DATE
);

CREATE TABLE IF NOT EXISTS company (
    name VARCHAR(225),
    link TEXT
);

CREATE TABLE IF NOT EXISTS education (
    required_credential VARCHAR(225)
);

CREATE TABLE IF NOT EXISTS experience (
    months_of_experience NUMERIC,
    seniority_level VARCHAR(25)
);

CREATE TABLE IF NOT EXISTS salary (
    currency VARCHAR(3),
    min_value NUMERIC,
    max_value NUMERIC,
    unit VARCHAR(12)
);

CREATE TABLE IF NOT EXISTS location (
    country VARCHAR(60),
    locality VARCHAR(60),
    region VARCHAR(60),
    postal_code VARCHAR(25),
    street_address VARCHAR(225),
    latitude NUMERIC,
    longitude NUMERIC
);
"""

def clean_non_alphabetic(input_string):
    # Check if the input is a string or can be converted to a string
    if not isinstance(input_string, (str, bytes)):
        return str(input_string)
    
    # Use a regular expression to keep only alphabetic characters and space
    return re.sub(r'[^a-zA-Z\s]', '', input_string)

def create_sql_statements(transformed_data):
    sql_statements = []

    # Insert into job table
    job_data = transformed_data.get("job", {})
    job_sql = (
        "INSERT INTO job (title, industry, description, employment_type, date_posted) VALUES ("
        "'{title}', '{industry}', '{description}', '{employment_type}', '{date_posted}')"
    ).format(
        title=clean_non_alphabetic(job_data.get('title', '')),
        industry=clean_non_alphabetic(job_data.get('industry', '')),
        description=clean_non_alphabetic(job_data.get('description', '').replace('{{br}}', '').replace('\n', '{{br}}')),
        employment_type=clean_non_alphabetic(job_data.get('employment_type', '')),
        date_posted=clean_non_alphabetic(job_data.get('date_posted', ''))
    )
    sql_statements.append(job_sql)

    # Insert into company table
    company_data = transformed_data.get("company", {})
    company_name = clean_non_alphabetic(company_data.get('name', ''))
    company_link = company_data.get('link', '')

    company_name_sql = 'NULL' if company_name is None or company_name == '' else f"'{company_name}'"
    company_link_sql = 'NULL' if company_link is None or company_link == '' else f"'{company_link}'"

    company_sql = f"INSERT INTO company (name, link) VALUES (" \
                f"{company_name_sql}, {company_link_sql})"
    sql_statements.append(company_sql)

    # Insert into education table
    education_data = transformed_data.get("education", {})
    required_credential = clean_non_alphabetic(education_data.get('required_credential', ''))

    required_credential_sql = 'NULL' if required_credential is None or required_credential == '' else f"'{required_credential}'"

    education_sql = f"INSERT INTO education (required_credential) VALUES (" \
                    f"{required_credential_sql})"
    sql_statements.append(education_sql)

    # Insert into experience table
    experience_data = transformed_data.get("experience", {})
    months_of_experience = experience_data.get('months_of_experience', 0)
    seniority_level = clean_non_alphabetic(experience_data.get('seniority_level', ''))

    months_of_experience_sql = 'NULL' if months_of_experience is None or months_of_experience == '' else months_of_experience
    seniority_level_sql = 'NULL' if seniority_level is None or seniority_level == '' else f"'{seniority_level}'"

    experience_sql = f"INSERT INTO experience (months_of_experience, seniority_level) VALUES (" \
                    f"{months_of_experience_sql}, {seniority_level_sql})"
    sql_statements.append(experience_sql)

    # Insert into salary table
    salary_data = transformed_data.get("salary", {})
    currency = salary_data.get('currency')
    min_value = salary_data.get('min_value')
    max_value = salary_data.get('max_value')
    unit = salary_data.get('unit')

    # Handle NULL values
    currency_sql = 'NULL' if currency is None or currency == '' else f"'{clean_non_alphabetic(currency)}'"
    min_value_sql = 'NULL' if min_value is None or min_value == '' else min_value
    max_value_sql = 'NULL' if max_value is None or max_value == '' else max_value
    unit_sql = 'NULL' if unit is None or unit == '' else f"'{clean_non_alphabetic(unit)}'"

    salary_sql = f"INSERT INTO salary (currency, min_value, max_value, unit) VALUES (" \
                f"{currency_sql}, {min_value_sql}, {max_value_sql}, {unit_sql})"


    sql_statements.append(salary_sql)

    # Insert into location table
    location_data = transformed_data.get("location", {})
    country = clean_non_alphabetic(location_data.get('country', ''))
    locality = clean_non_alphabetic(location_data.get('locality', ''))
    region = clean_non_alphabetic(location_data.get('region', ''))
    postal_code = clean_non_alphabetic(location_data.get('postal_code', ''))
    street_address = location_data.get('street_address', '')
    latitude = location_data.get('latitude', 0)
    longitude = location_data.get('longitude', 0)

    country_sql = 'NULL' if country is None or country == '' else f"'{country}'"
    locality_sql = 'NULL' if locality is None or locality == '' else f"'{locality}'"
    region_sql = 'NULL' if region is None or region == '' else f"'{region}'"
    postal_code_sql = 'NULL' if postal_code is None or postal_code == '' else f"'{postal_code}'"
    street_address_sql = 'NULL' if street_address is None or street_address == '' else f"'{street_address}'"
    latitude_sql = 'NULL' if latitude is None else latitude
    longitude_sql = 'NULL' if longitude is None else longitude

    location_sql = f"INSERT INTO location (country, locality, region, postal_code, street_address, " \
                f"latitude, longitude) VALUES (" \
                f"{country_sql}, {locality_sql}, {region_sql}, {postal_code_sql}, {street_address_sql}, " \
                f"{latitude_sql}, {longitude_sql})"
    sql_statements.append(location_sql)

    return sql_statements


def extract(source_file, target_dir):
    """Extract data from jobs.csv."""
    print("CALLING EXTRACT FUNCTION")
    df = pd.read_csv(source_file)
    context_data = df['context'].tolist()

    print(f"Total records in CSV: {len(context_data)}")

    for idx, data in enumerate(context_data):
        if isinstance(data, str):
            try:
                json_data = json.load(io.StringIO(data))
                output_file_path = f'{target_dir}/extracted_{idx}.json'
                with open(output_file_path, 'w') as file:
                    json.dump(json_data, file, indent=2)
                print(f"File extracted: {output_file_path}")
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON at index {idx}: {e}")
        else:
            print(f"Ignoring non-string data at index {idx}")

    print("Extraction completed.")

def transform(extracted_dir, transformed_dir):
    """Clean and convert extracted elements to json."""
    idx = 0

    while True:
        input_file = f'{extracted_dir}/extracted_{idx}.json'

        if not os.path.exists(input_file):
            # Check for the next available file
            found_next_file = False
            while not os.path.exists(input_file):
                idx += 1
                input_file = f'{extracted_dir}/extracted_{idx}.json'

                # If no more files are found, exit the loop
                if not os.path.exists(input_file):
                    found_next_file = False
                    break
                found_next_file = True

            # Break out of the loop if no more files are found
            if not found_next_file:
                break
            else:
                continue  # Skip to the next iteration
    
        with open(input_file, 'r') as file:
            data = json.load(file)

        # Handle the case when 'monthsOfExperience' is not found
        try:
            months_of_experience = data.get("experienceRequirements", {}).get("monthsOfExperience")
        except AttributeError:
            months_of_experience = None  # Set it to None or any other default value

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
                "months_of_experience": months_of_experience,
                "seniority_level": "",
            },  
            "salary": {
                "currency": data.get("estimatedSalary", {}).get("currency", ""),  
                "min_value": data.get("estimatedSalary", {}).get("value", {}).get("minValue", ""),  
                "max_value": data.get("estimatedSalary", {}).get("value", {}).get("maxValue", ""),  
                "unit": data.get("estimatedSalary", {}).get("value", {}).get("unitText", ""),  
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

        idx += 1
            

def load(transformed_dir):
    """Load data to SQLite database using Airflow SQLiteHook."""
    
    # Create a SQLiteHook to interact with the database
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')

    # Split the TABLES_CREATION_QUERY into individual statements
    create_statements = TABLES_CREATION_QUERY.split(';')
    for create_statement in create_statements:
        if create_statement.strip():
            # Execute SQL statements using the SQLiteHook
            sqlite_hook.run(sql=create_statement)

    idx = 0
    
    while idx:  
        input_file = f'{transformed_dir}/transformed_{idx}.json'
        if not os.path.exists(input_file):
            break

        with open(input_file, 'r') as file:
            transformed_data = json.load(file)

        sql_statements = create_sql_statements(transformed_data)
        for statement in sql_statements:
            # Execute SQL statements using the SQLiteHook
            sqlite_hook.run(sql=statement)
        idx+=1
        
def create_tables():
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    statements = TABLES_CREATION_QUERY.split(';')
    for statement in statements:
        if statement.strip():
            sqlite_hook.run(sql=statement)
            
            
            
DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

dag = DAG(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)

create_tables_task = PythonOperator(
    task_id="create_tables",
    python_callable=create_tables,
    dag=dag
)

source_file = '/mnt/c/Users/Admin/Documents/Data-Internship-Home-Assignment/source/jobs.csv'
target_dir = '/mnt/c/Users/Admin/Documents/Data-Internship-Home-Assignment/staging/'

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract,
    op_args=[source_file, target_dir],
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform,
    op_args=[f'{target_dir}/extracted', f'{target_dir}/transformed'],
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load,
    op_args=[f'{target_dir}/transformed'],
    provide_context=True,
    dag=dag,
)

create_tables_task >> extract_task >> transform_task >> load_task
