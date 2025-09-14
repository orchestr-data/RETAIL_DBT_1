from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.empty import EmptyOperator

# Configuration for all 5 datasets - ALL using compression for this project
PIPELINE_CONFIGS = {
    'customers': {
        'glue_job': 'customers_compression',
        'needs_compression': True,
        'staging_model': 'stg_cust_details',
        'mart_model': 'dim_customers',
        'file_format': 'csv_gzip_format'
    },
    'products': {
        'glue_job': 'products_to_csv+gzip',
        'needs_compression': True,  # Using compression for consistency in this project
        'staging_model': 'stg_products',
        'mart_model': 'dim_products',
        'file_format': 'csv_gzip_format'  # All files are gzipped
    },
    'stores': {
        'glue_job': 'stores_to_csv+gzip',
        'needs_compression': True,  # Using compression for consistency in this project
        'staging_model': 'stg_stores',
        'mart_model': 'dim_stores',
        'file_format': 'csv_gzip_format'  # All files are gzipped
    },
    'sales': {
        'glue_job': 'sales_to_csv+gzip',
        'needs_compression': True,
        'staging_model': 'stg_sales',
        'mart_model': 'fact_sales',
        'file_format': 'csv_gzip_format'
    },
    'shipments': {
        'glue_job': 'shipments_to_csv+gzip',
        'needs_compression': True,
        'staging_model': 'stg_shipments',
        'mart_model': 'fact_shipments',
        'file_format': 'csv_gzip_format'
    }
}

def create_data_pipeline_dag(source_name, config):
    """
    Factory function to create DAG for any data source
    """
    default_args = {
        'owner': 'data-team',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    }

    dag = DAG(
        dag_id=f"{source_name}_data_pipeline",
        default_args=default_args,
        description=f"{source_name.title()}: S3 → Glue ETL → Snowflake → dbt",
        schedule_interval="@daily",
        catchup=False,
        tags=[source_name, "glue", "snowflake", "dbt"],
    )

    with dag:
        # Start task
        start_pipeline = EmptyOperator(task_id=f"start_{source_name}_pipeline")

        # Step 1: Glue ETL job (all datasets use Glue in your project)
        glue_process = GlueJobOperator(
            task_id=f"glue_process_{source_name}_data",
            job_name=config['glue_job'],  # Use your actual job names
            aws_conn_id="aws_default",
            iam_role_name="GlueS3AccessRole"
        )

        # Step 2a: Load to Snowflake
        load_to_snowflake = SnowflakeOperator(
            task_id=f"load_{source_name}_to_snowflake",
            sql=f"""
            COPY INTO raw.{source_name}
            FROM @my_stage/{source_name}/
            FILE_FORMAT = (FORMAT_NAME = {config['file_format']});
            """,
            snowflake_conn_id="snowflake_default"
        )

        # Step 2b: Count records (audit/validation)
        count_records = SnowflakeOperator(
            task_id=f"count_{source_name}",
            sql=f"SELECT COUNT(*) AS {source_name}_count FROM raw.{source_name};",
            snowflake_conn_id="snowflake_default",
            do_xcom_push=True
        )

        # Step 3: dbt staging
        dbt_staging = BashOperator(
            task_id=f"dbt_{source_name}_staging",
            bash_command=f"cd /opt/airflow/dbt && dbt run --select {config['staging_model']}"
        )

        # Step 4: dbt marts  
        dbt_marts = BashOperator(
            task_id=f"dbt_{source_name}_marts",
            bash_command=f"cd /opt/airflow/dbt && dbt run --select {config['mart_model']}"
        )

        # End task
        end_pipeline = EmptyOperator(task_id=f"end_{source_name}_pipeline")

        # DAG flow - same pattern for all datasets
        start_pipeline >> glue_process >> load_to_snowflake >> count_records
        count_records >> dbt_staging >> dbt_marts >> end_pipeline

    return dag

# Generate all 5 DAGs automatically
for source_name, config in PIPELINE_CONFIGS.items():
    globals()[f"{source_name}_data_pipeline"] = create_data_pipeline_dag(source_name, config)