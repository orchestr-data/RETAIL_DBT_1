"""
Customers Data Pipeline DAG with Astro Cosmos
Flow: S3 Raw -> S3 Compressed -> Snowflake -> dbt Transformations
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.dummy import DummyOperator

# Cosmos imports for dbt
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# DAG definition
dag = DAG(
    'customers_data_pipeline',
    default_args=default_args,
    description='Customers data pipeline with Cosmos dbt integration',
    schedule_interval='@daily',
    max_active_runs=1,
    tags=['retail', 'customers', 'etl', 'dbt', 'cosmos']
)

# dbt configuration
profile_config = ProfileConfig(
    profile_name="retail_project",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={
            "database": "RETAIL_PROJECT",
            "schema": "RAW"
        }
    )
)

project_config = ProjectConfig(
    dbt_project_path="/usr/local/airflow/include/retail_project",
)

execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/airflow/.venv/bin/dbt",
)

# Start task
start_pipeline = DummyOperator(
    task_id='start_customers_pipeline',
    dag=dag
)

# Step 1: Copy and compress customers file
compress_customers_file = S3CopyObjectOperator(
    task_id='compress_customers_file',
    source_bucket_name='rohith-retail-bucket',
    source_bucket_key='rohith-retail-bucket/customers/{{ ds }}/customers.csv',
    dest_bucket_name='rohith-bucket-compressed', 
    dest_bucket_key='rohith-bucket-compressed/customers/{{ ds }}/*.csv.gz',
    aws_conn_id='aws_default',
    dag=dag
)

# Step 2: Load customers data into Snowflake staging
load_customers_to_staging = SnowflakeOperator(
    task_id='load_customers_to_staging',
    snowflake_conn_id='snowflake_default',
    sql="""
    COPY INTO RETAIL_PROJECT.RAW.CUSTOMERS
    FROM @my_stage/customers/{{ ds }}/
    FILE_FORMAT = (FORMAT_NAME = 'csv_gzip_format')
    PATTERN = '.*\\.csv.*'
    ON_ERROR = 'CONTINUE';
    """,
    dag=dag
)

# Step 3: dbt transformations with Cosmos - all in one task group
dbt_transformations = DbtTaskGroup(
    group_id="dbt_customers_transformations",
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    default_args={"retries": 2},
    operator_args={
        "vars": {
            "start_date": "{{ ds }}",
            "end_date": "{{ ds }}"
        }
    },
    # Run specific models for customers pipeline
    select=["stg_cust_detail", "dim_customers"],
    test_behavior="after_each",  # Run tests after each model
    dag=dag
)

# End task
end_pipeline = DummyOperator(
    task_id='end_customers_pipeline',
    dag=dag
)

# Define dependencies
start_pipeline >> compress_customers_file >> load_customers_to_staging >> dbt_transformations >> end_pipeline