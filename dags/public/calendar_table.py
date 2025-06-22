import pandas as pd
import holidays
import logging
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID = "postgres_default"
SCHEMA_NAME = "public"
TABLE_NAME = "calendar"


@dag(
    dag_id="public_calendar_table",
    schedule=None,
    start_date=datetime(2025, 6, 22),
    catchup=False,
    tags=["public"],
)
def create_calendar_table_dag():
    @task()
    def generate_calendar(start_date: str = None, end_date: str = None):
        dates = pd.date_range(start=start_date, end=end_date)
        df = pd.DataFrame({"date": dates})
        holiday_dates = set(
            pd.to_datetime(
                list(
                    holidays.country_holidays(
                        "US", years=range(pd.to_datetime(start_date).year, pd.to_datetime(end_date).year + 1)
                    )
                ).copy()
            )
        )
        df["day"] = df["date"].dt.day
        df["month"] = df["date"].dt.month
        df["month_name"] = df["date"].dt.strftime("%b")
        df["month_full_name"] = df["date"].dt.strftime("%B")
        df["year"] = df["date"].dt.year
        df["quarter"] = df["date"].dt.quarter
        df["semester"] = df["date"].dt.month.apply(lambda m: 1 if m <= 6 else 2)
        df["weekday"] = df["date"].dt.weekday
        df["weekday_name"] = df["date"].dt.strftime("%A")
        df["is_holiday"] = df["date"].isin(holiday_dates)
        df["is_weekend"] = df["date"].dt.weekday >= 5

        # Convert pandas Timestamp to Python date for serialization
        df["date"] = df["date"].dt.date

        return df

    @task()
    def create_table():
        hook = PostgresHook(postgres_conn_id="postgres_default")
        create_sql = """
        CREATE TABLE IF NOT EXISTS calendar (
            date DATE PRIMARY KEY,
            day INTEGER NOT NULL,
            month INTEGER NOT NULL,
            month_name VARCHAR(3) NOT NULL,
            month_full_name VARCHAR(20) NOT NULL,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            semester INTEGER NOT NULL,
            weekday INTEGER NOT NULL,
            weekday_name VARCHAR(10) NOT NULL,
            is_holiday BOOLEAN NOT NULL,
            is_weekend BOOLEAN NOT NULL
        )
        """
        hook.run(create_sql)

    @task()
    def insert_calendar(df: pd.DataFrame):
        logger = logging.getLogger("airflow.task")

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        logger.info(f"Writing DataFrame to Postgres table '{TABLE_NAME}' (replace mode).")

        df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False, schema=SCHEMA_NAME)
        logger.info("Data successfully loaded into Postgres.")

    create_table()
    calendar_records = generate_calendar("2000-01-01", "2027-12-31")
    insert_calendar(calendar_records)


# Instantiate the DAG
create_calendar_table_dag()
