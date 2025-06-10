import pandas as pd
import logging
from sodapy import Socrata
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta


POSTGRES_CONN_ID = "postgres_default"
TABLE_NAME = "fire_incidents"


@dag(
    dag_id="datasf_fire_incidents_incremental_load",
    schedule="@daily",
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=["etl", "datasf", "sfgov", "fire_incidents"],
)
def initial_loader():
    @task()
    def download_csv(**context):
        logger = logging.getLogger("airflow.task")
        logger.info("Starting incremental download of Fire Incidents data from SFGOV Open Data.")

        client = Socrata("data.sfgov.org", None)

        # Get yesterday's date in ISO format
        # Use Airflow's logical_date (execution_date) for incremental load
        extract_date = (context["logical_date"] - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%S.%f")

        # Query for records where 'incident_datetime' is greater than yesterday
        logger.info(f"Querying SFGOV Open Data for records with data_as_of >= '{extract_date}'")
        results = []
        offset = 0
        limit = 1000
        total_fetched = 0
        while True:
            logger.info(f"Fetching batch: offset={offset}, limit={limit}")
            batch = client.get("wr8u-xric", where=f"data_as_of >= '{extract_date}'", limit=limit, offset=offset)
            logger.info(f"Fetched {len(batch)} records in this batch.")
            if not batch:
                logger.info("No more records to fetch.")
                break
            results.extend(batch)
            total_fetched += len(batch)
            if len(batch) < limit:
                logger.info("Last batch fetched; exiting loop.")
                break
            offset += limit
        logger.info(f"Total records fetched: {total_fetched}")

        return results

    @task()
    def load_to_postgres(items: list):
        logger = logging.getLogger("airflow.task")

        if not items:
            logger.info("No items to load. Skipping database upsert.")
            return

        df = pd.DataFrame(items)
        logger.info(f"Loaded data into DataFrame with shape: {df.shape}")

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        conn = engine.connect()
        trans = conn.begin()
        try:
            # Use PostgresHook's insert_rows with upsert (ON CONFLICT) logic
            logger.info("Preparing data for upsert using PostgresHook.")

            # Convert DataFrame to list of tuples
            records = df.to_records(index=False)
            rows = list(records)

            # Build upsert (ON CONFLICT) clause
            conflict_fields = ["id"]

            # Build target fields
            target_fields = [col for col in df.columns]

            pg_hook.insert_rows(
                table=f"datasf.{TABLE_NAME}",
                rows=rows,
                target_fields=target_fields,
                commit_every=1000,
                replace=True,
                replace_index=conflict_fields,
            )
            trans.commit()
            logger.info("Upsert completed successfully using PostgresHook.")
        except Exception as e:
            trans.rollback()
            logger.error(f"Error during upsert: {e}")
            raise
        finally:
            conn.close()

    items_list = download_csv()
    load_task = load_to_postgres(items_list)

    items_list >> load_task


# Instantiate the DAG
initial_loader()
