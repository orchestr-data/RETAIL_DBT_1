from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.empty import EmptyOperator

# Configuration for promotions, refunds, returns - Direct S3 to Snowflake (JSON files)
DIRECT_LOAD_CONFIGS = {
    'promotions': {
        'staging_model': 'stg_promotions',
        'mart_model': 'dim_promotions',
        'file_format': 'json_format'  # JSON files
    },
    'refunds': {
        'staging_model': 'stg_refunds', 
        'mart_model': 'fact_refunds',
        'file_format': 'json_format'  # JSON files
    },
    'returns': {
        'staging_model': 'stg_returns',
        'mart_model': 'fact_returns', 
        'file_format': 'json_format'  # JSON files
    }
}

def create_direct_load_dag(source_name, config):
    """
    Factory function to create DAG for direct S3 → Snowflake → dbt (Docker) pipeline
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
        description=f"{source_name.title()}: S3 → Snowflake → dbt Docker (Direct Load)",
        schedule_interval="@daily",
        catchup=False,
        tags=[source_name, "snowflake", "dbt", "direct-load", "docker"],
    )

    with dag:
        # Start task
        start_pipeline = EmptyOperator(task_id=f"start_{source_name}_pipeline")

        # Step 1: Direct load from S3 to Snowflake (skip Glue)
        load_to_snowflake = SnowflakeOperator(
            task_id=f"load_{source_name}_to_snowflake",
            sql=f"""
            COPY INTO raw.{source_name}
            FROM @stage_not_compressed/{source_name}/
            FILE_FORMAT = (FORMAT_NAME = {config['file_format']});
            """,
            snowflake_conn_id="snowflake_default"
        )

        # Step 2: Count records (audit/validation)
        count_records = SnowflakeOperator(
            task_id=f"count_{source_name}",
            sql=f"SELECT COUNT(*) AS {source_name}_count FROM raw.{source_name};",
            snowflake_conn_id="snowflake_default",
            do_xcom_push=True
        )

        # Step 3: dbt staging (via Docker - no dependency conflicts!)
        dbt_staging = BashOperator(
            task_id=f"dbt_{source_name}_staging",
            bash_command=f"""
            docker run --rm \
              -e DBT_PROFILES_DIR=/root/.dbt \
              -e SNOWFLAKE_ACCOUNT={{{{ var.value.SNOWFLAKE_ACCOUNT }}}} \
              -e SNOWFLAKE_USER={{{{ var.value.SNOWFLAKE_USER }}}} \
              -e SNOWFLAKE_PASSWORD={{{{ var.value.SNOWFLAKE_PASSWORD }}}} \
              -e SNOWFLAKE_DATABASE={{{{ var.value.SNOWFLAKE_DATABASE }}}} \
              -e SNOWFLAKE_WAREHOUSE={{{{ var.value.SNOWFLAKE_WAREHOUSE }}}} \
              -v /opt/airflow/dbt:/usr/app \
              -w /usr/app \
              dbt/dbt-snowflake:1.7.1 \
              dbt run --select {config['staging_model']}
            """
        )

        # Step 4: dbt marts (via Docker)
        dbt_marts = BashOperator(
            task_id=f"dbt_{source_name}_marts",
            bash_command=f"""
            docker run --rm \
              -e DBT_PROFILES_DIR=/root/.dbt \
              -e SNOWFLAKE_ACCOUNT={{{{ var.value.SNOWFLAKE_ACCOUNT }}}} \
              -e SNOWFLAKE_USER={{{{ var.value.SNOWFLAKE_USER }}}} \
              -e SNOWFLAKE_PASSWORD={{{{ var.value.SNOWFLAKE_PASSWORD }}}} \
              -e SNOWFLAKE_DATABASE={{{{ var.value.SNOWFLAKE_DATABASE }}}} \
              -e SNOWFLAKE_WAREHOUSE={{{{ var.value.SNOWFLAKE_WAREHOUSE }}}} \
              -v /opt/airflow/dbt:/usr/app \
              -w /usr/app \
              dbt/dbt-snowflake:1.7.1 \
              dbt run --select {config['mart_model']}
            """
        )

        # End task
        end_pipeline = EmptyOperator(task_id=f"end_{source_name}_pipeline")

        # DAG flow - Direct pipeline without Glue
        start_pipeline >> load_to_snowflake >> count_records
        count_records >> dbt_staging >> dbt_marts >> end_pipeline

    return dag

# Generate all 3 DAGs automatically
for source_name, config in DIRECT_LOAD_CONFIGS.items():
    globals()[f"{source_name}_data_pipeline"] = create_direct_load_dag(source_name, config)