import pandas as pd
import logging
import time
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
        start_time = time.time()
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
        batch_count = 0

        while True:
            batch_start = time.time()
            logger.info(f"Fetching batch {batch_count+1}: offset={offset}, limit={limit}")

            batch = client.get("wr8u-xric", where=f"data_as_of >= '{extract_date}'", limit=limit, offset=offset)
            batch_count += 1
            batch_size = len(batch)
            batch_duration = time.time() - batch_start

            logger.info(f"Batch {batch_count}: Fetched {batch_size} records in {batch_duration:.2f} seconds.")

            if not batch:
                logger.info("No more records to fetch.")
                break

            results.extend(batch)
            total_fetched += batch_size

            if batch_size < limit:
                logger.info("Last batch fetched; exiting loop.")
                break

            offset += limit

        # Log validation counts
        unique_incidents = len(set(item.get("incident_number") for item in results if "incident_number" in item))

        duration = time.time() - start_time
        logger.info(f"Download completed in {duration:.2f} seconds.")
        logger.info(f"Total records fetched: {total_fetched} across {batch_count} batches")
        logger.info(f"Unique incident numbers: {unique_incidents}")

        return {
            "data": results,
            "metadata": {
                "total_records": total_fetched,
                "unique_incidents": unique_incidents,
                "extract_date": extract_date,
                "batch_count": batch_count,
                "duration_seconds": duration,
            },
        }

    @task()
    def load_to_postgres(download_result: dict):
        logger = logging.getLogger("airflow.task")
        start_time = time.time()

        items = download_result["data"]
        metadata = download_result["metadata"]

        logger.info(f"Starting database load process with {metadata['total_records']} records.")
        logger.info(f"Extract date: {metadata['extract_date']}, Unique incidents: {metadata['unique_incidents']}")

        if not items:
            logger.info("No items to load. Skipping database upsert.")
            return {"records_processed": 0, "duration_seconds": time.time() - start_time}

        df = pd.DataFrame(items)
        logger.info(f"Loaded data into DataFrame with shape: {df.shape}")

        # Validate data before loading
        missing_id_count = df["id"].isna().sum() if "id" in df.columns else "id column not found"
        logger.info(f"Data validation - Missing IDs: {missing_id_count}")

        df_columns = set(df.columns)
        logger.info(f"Columns in DataFrame: {len(df_columns)}")

        # Start database operations with timing
        db_start_time = time.time()
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()

        # Get current count before insert
        pre_count_query = f"SELECT COUNT(*) FROM datasf.{TABLE_NAME}"
        pre_count = pg_hook.get_first(pre_count_query)[0]
        logger.info(f"Pre-upsert row count in table: {pre_count}")

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

            # Log before upsert
            logger.info(f"Attempting upsert with {len(rows)} rows and {len(target_fields)} columns")
            logger.info(f"Using conflict fields: {conflict_fields}")

            upsert_start = time.time()
            pg_hook.insert_rows(
                table=f"datasf.{TABLE_NAME}",
                rows=rows,
                target_fields=target_fields,
                commit_every=1000,
                replace=True,
                replace_index=conflict_fields,
            )
            upsert_duration = time.time() - upsert_start
            trans.commit()
            logger.info(f"Upsert completed successfully in {upsert_duration:.2f} seconds")

            # Get post-upsert count
            post_count_query = f"SELECT COUNT(*) FROM datasf.{TABLE_NAME}"
            post_count = pg_hook.get_first(post_count_query)[0]
            net_change = post_count - pre_count

            logger.info(f"Post-upsert row count: {post_count} (net change: {net_change})")

            # Get counts of updated vs inserted records
            total_processed = len(rows)
            inserted = net_change
            updated = total_processed - inserted if inserted >= 0 else "Unknown"

            logger.info(f"Records processed: {total_processed}, Inserted: {inserted}, Updated: {updated}")

        except Exception as e:
            trans.rollback()
            logger.error(f"Error during upsert: {e}")
            raise
        finally:
            conn.close()

        total_duration = time.time() - start_time
        db_duration = time.time() - db_start_time
        logger.info(f"Database operations completed in {db_duration:.2f} seconds")
        logger.info(f"Total task duration: {total_duration:.2f} seconds")

        return {
            "records_processed": len(rows),
            "pre_count": pre_count,
            "post_count": post_count,
            "net_change": net_change,
            "duration_seconds": total_duration,
        }

    download_result = download_csv()
    load_result = load_to_postgres(download_result)

    download_result >> load_result


# Instantiate the DAG
initial_loader()
