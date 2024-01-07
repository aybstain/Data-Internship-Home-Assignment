def create_sql_statements(transformed_data):
    sql_statements = []

    # Insert into job table
    job_data = transformed_data.get("job", {})
    job_sql = f"INSERT INTO job (title, industry, description, employment_type, date_posted) VALUES (" \
              f"'{job_data.get('title', '')}', '{job_data.get('industry', '')}', '{job_data.get('description', '')}', " \
              f"'{job_data.get('employment_type', '')}', '{job_data.get('date_posted', '')}')"
    sql_statements.append(job_sql)

    # Get the last inserted job_id
    job_id = "(SELECT last_insert_rowid())"

    # Insert into company table
    company_data = transformed_data.get("company", {})
    company_sql = f"INSERT INTO company (job_id, name, link) VALUES ({job_id}, " \
                  f"'{company_data.get('name', '')}', '{company_data.get('link', '')}')"
    sql_statements.append(company_sql)

    # Insert into education table
    education_data = transformed_data.get("education", {})
    education_sql = f"INSERT INTO education (job_id, required_credential) VALUES ({job_id}, " \
                    f"'{education_data.get('required_credential', '')}')"
    sql_statements.append(education_sql)

    # Insert into experience table
    experience_data = transformed_data.get("experience", {})
    experience_sql = f"INSERT INTO experience (job_id, months_of_experience, seniority_level) VALUES ({job_id}, " \
                     f"{experience_data.get('months_of_experience', 0)}, '{experience_data.get('seniority_level', '')}')"
    sql_statements.append(experience_sql)

    # Insert into salary table
    salary_data = transformed_data.get("salary", {})
    salary_sql = f"INSERT INTO salary (job_id, currency, min_value, max_value, unit) VALUES ({job_id}, " \
                 f"'{salary_data.get('currency', '')}', {salary_data.get('min_value', 0)}, " \
                 f"{salary_data.get('max_value', 0)}, '{salary_data.get('unit', '')}')"
    sql_statements.append(salary_sql)

    # Insert into location table
    location_data = transformed_data.get("location", {})
    location_sql = f"INSERT INTO location (job_id, country, locality, region, postal_code, street_address, " \
                   f"latitude, longitude) VALUES ({job_id}, '{location_data.get('country', '')}', " \
                   f"'{location_data.get('locality', '')}', '{location_data.get('region', '')}', " \
                   f"'{location_data.get('postal_code', '')}', '{location_data.get('street_address', '')}', " \
                   f"{location_data.get('latitude', 0)}, {location_data.get('longitude', 0)})"
    sql_statements.append(location_sql)

    return sql_statements
