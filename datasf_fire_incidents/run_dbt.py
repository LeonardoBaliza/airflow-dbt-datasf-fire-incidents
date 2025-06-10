import os
from datetime import datetime
from airflow.sdk import dag, chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

CONNECTION_ID = "postgres_default"
DB_NAME = "postgres"
SCHEMA_NAME = "datasf"
MODEL_TO_QUERY = "agg_count_by_time"
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/fire"
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="default",
    target_name=os.environ.get("DBT_TARGET", "dev"),
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=CONNECTION_ID,
        profile_args={"schema": SCHEMA_NAME},
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)


@dag(
    dag_id="datasf_fire_incidents_run_dbt",
    schedule=None,
    start_date=datetime(2025, 6, 8),
    catchup=False,
    tags=["etl", "datasf", "sfgov", "fire_incidents", "dbt"],
)
def my_simple_dbt_dag():
    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
    )

    query_agg_count_by_time = SQLExecuteQueryOperator(
        task_id="query_agg_count_by_time",
        conn_id=CONNECTION_ID,
        sql=f"SELECT * FROM {DB_NAME}.{SCHEMA_NAME}.{MODEL_TO_QUERY} LIMIT 100",
    )

    chain(transform_data, query_agg_count_by_time)


my_simple_dbt_dag()
