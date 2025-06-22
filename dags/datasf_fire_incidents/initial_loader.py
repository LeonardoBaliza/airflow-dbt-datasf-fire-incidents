import requests
import pandas as pd
import os
import tempfile
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging


POSTGRES_CONN_ID = "postgres_default"
TABLE_NAME = "fire_incidents"
COLUMNS_DICT = {
    "Incident Number": "incident_number",
    "Exposure Number": "exposure_number",
    "ID": "id",
    "Address": "address",
    "Incident Date": "incident_date",
    "Call Number": "call_number",
    "Alarm DtTm": "alarm_dttm",
    "Arrival DtTm": "arrival_dttm",
    "Close DtTm": "close_dttm",
    "City": "city",
    "zipcode": "zipcode",
    "Battalion": "battalion",
    "Station Area": "station_area",
    "Box": "box",
    "Suppression Units": "suppression_units",
    "Suppression Personnel": "suppression_personnel",
    "EMS Units": "ems_units",
    "EMS Personnel": "ems_personnel",
    "Other Units": "other_units",
    "Other Personnel": "other_personnel",
    "First Unit On Scene": "first_unit_on_scene",
    "Estimated Property Loss": "estimated_property_loss",
    "Estimated Contents Loss": "estimated_contents_loss",
    "Fire Fatalities": "fire_fatalities",
    "Fire Injuries": "fire_injuries",
    "Civilian Fatalities": "civilian_fatalities",
    "Civilian Injuries": "civilian_injuries",
    "Number of Alarms": "number_of_alarms",
    "Primary Situation": "primary_situation",
    "Mutual Aid": "mutual_aid",
    "Action Taken Primary": "action_taken_primary",
    "Action Taken Secondary": "action_taken_secondary",
    "Action Taken Other": "action_taken_other",
    "Detector Alerted Occupants": "detector_alerted_occupants",
    "Property Use": "property_use",
    "Area of Fire Origin": "area_of_fire_origin",
    "Ignition Cause": "ignition_cause",
    "Ignition Factor Primary": "ignition_factor_primary",
    "Ignition Factor Secondary": "ignition_factor_secondary",
    "Heat Source": "heat_source",
    "Item First Ignited": "item_first_ignited",
    "Human Factors Associated with Ignition": "human_factors_associated_with_ignition",
    "Structure Type": "structure_type",
    "Structure Status": "structure_status",
    "Floor of Fire Origin": "floor_of_fire_origin",
    "Fire Spread": "fire_spread",
    "No Flame Spread": "no_flame_spread",
    "Number of floors with minimum damage": "number_of_floors_with_minimum_damage",
    "Number of floors with significant damage": "number_of_floors_with_significant_damage",
    "Number of floors with heavy damage": "number_of_floors_with_heavy_damage",
    "Number of floors with extreme damage": "number_of_floors_with_extreme_damage",
    "Detectors Present": "detectors_present",
    "Detector Type": "detector_type",
    "Detector Operation": "detector_operation",
    "Detector Effectiveness": "detector_effectiveness",
    "Detector Failure Reason": "detector_failure_reason",
    "Automatic Extinguishing System Present": "automatic_extinguishing_system_present",
    "Automatic Extinguishing Sytem Type": "automatic_extinguishing_sytem_type",
    "Automatic Extinguishing Sytem Perfomance": "automatic_extinguishing_sytem_perfomance",
    "Automatic Extinguishing Sytem Failure Reason": "automatic_extinguishing_sytem_failure_reason",
    "Number of Sprinkler Heads Operating": "number_of_sprinkler_heads_operating",
    "Supervisor District": "supervisor_district",
    "neighborhood_district": "neighborhood_district",
    "point": "point",
    "data_as_of": "data_as_of",
    "data_loaded_at": "data_loaded_at",
}
COLUMNS_TYPE = {
    "incident_number": "text",
    "exposure_number": "number",
    "id": "text",
    "address": "text",
    "incident_date": "calendar_date",
    "call_number": "text",
    "alarm_dttm": "calendar_date",
    "arrival_dttm": "calendar_date",
    "close_dttm": "calendar_date",
    "city": "text",
    "zipcode": "text",
    "battalion": "text",
    "station_area": "text",
    "box": "text",
    "suppression_units": "number",
    "suppression_personnel": "number",
    "ems_units": "number",
    "ems_personnel": "number",
    "other_units": "number",
    "other_personnel": "number",
    "first_unit_on_scene": "text",
    "estimated_property_loss": "number",
    "estimated_contents_loss": "number",
    "fire_fatalities": "number",
    "fire_injuries": "number",
    "civilian_fatalities": "number",
    "civilian_injuries": "number",
    "number_of_alarms": "number",
    "primary_situation": "text",
    "mutual_aid": "text",
    "action_taken_primary": "text",
    "action_taken_secondary": "text",
    "action_taken_other": "text",
    "detector_alerted_occupants": "text",
    "property_use": "text",
    "area_of_fire_origin": "text",
    "ignition_cause": "text",
    "ignition_factor_primary": "text",
    "ignition_factor_secondary": "text",
    "heat_source": "text",
    "item_first_ignited": "text",
    "human_factors_associated_with_ignition": "text",
    "structure_type": "text",
    "structure_status": "text",
    "floor_of_fire_origin": "number",
    "fire_spread": "text",
    "no_flame_spread": "text",
    "number_of_floors_with_minimum_damage": "number",
    "number_of_floors_with_significant_damage": "number",
    "number_of_floors_with_heavy_damage": "number",
    "number_of_floors_with_extreme_damage": "number",
    "detectors_present": "text",
    "detector_type": "text",
    "detector_operation": "text",
    "detector_effectiveness": "text",
    "detector_failure_reason": "text",
    "automatic_extinguishing_system_present": "text",
    "automatic_extinguishing_sytem_type": "text",
    "automatic_extinguishing_sytem_perfomance": "text",
    "automatic_extinguishing_sytem_failure_reason": "text",
    "number_of_sprinkler_heads_operating": "number",
    "supervisor_district": "text",
    "neighborhood_district": "text",
    "point": "point",
    "data_as_of": "calendar_date",
    "data_loaded_at": "calendar_date",
}


@dag(
    dag_id="datasf_fire_incidents_initial_loader",
    schedule=None,
    start_date=datetime(2025, 6, 8),
    catchup=False,
    tags=["etl", "datasf", "sfgov", "fire_incidents"],
)
def initial_loader():
    @task()
    def create_schema():
        logger = logging.getLogger("airflow.task")
        logger.info("Creating schema 'datasf' in Postgres if it does not exist.")
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = "CREATE SCHEMA IF NOT EXISTS datasf;"
        pg_hook.run(sql)
        logger.info("Schema 'datasf' ensured to exist.")

    @task()
    def create_table():
        logger = logging.getLogger("airflow.task")
        logger.info(f"Creating table '{TABLE_NAME}' in schema 'datasf' if it does not exist.")

        columns_sql = []
        for col, col_type in COLUMNS_TYPE.items():
            if col_type == "number":
                sql_type = "NUMERIC"
            elif col_type == "calendar_date":
                sql_type = "TIMESTAMP"
            elif col_type == "point":
                sql_type = "TEXT"
            else:
                sql_type = "TEXT"
            if col == "id":
                columns_sql.append(f'"{col}" {sql_type} UNIQUE')
            else:
                columns_sql.append(f'"{col}" {sql_type}')

        columns_str = ",\n    ".join(columns_sql)
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS datasf.{TABLE_NAME} (
            {columns_str}
        );
        """

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        pg_hook.run(create_table_sql)
        logger.info(f"Table '{TABLE_NAME}' ensured to exist in schema 'datasf'.")

    @task()
    def download_csv():
        logger = logging.getLogger("airflow.task")
        logger.info("Starting download of Fire Incidents CSV from SFGOV Open Data.")

        today = datetime.now().strftime("%Y%m%d")
        url = f"https://data.sfgov.org/api/views/wr8u-xric/rows.csv?date={today}&accessType=DOWNLOAD"
        logger.info(f"Downloading data from URL: {url}")
        response = requests.get(url)
        response.raise_for_status()
        tmp_dir = tempfile.gettempdir()
        file_path = os.path.join(tmp_dir, f"Fire_Incidents_{today}.csv")
        with open(file_path, "wb") as f:
            f.write(response.content)
        logger.info(f"CSV file saved to: {file_path}")
        return file_path

    @task()
    def load_to_postgres(file_path: str):

        logger = logging.getLogger("airflow.task")
        logger.info(f"Loading CSV file from: {file_path}")

        df = pd.read_csv(file_path, low_memory=False)
        logger.info(f"CSV loaded into DataFrame with shape: {df.shape}")

        df = df.rename(columns=COLUMNS_DICT)
        logger.info("Columns renamed according to COLUMNS_DICT mapping.")

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        logger.info(f"Writing DataFrame to Postgres table '{TABLE_NAME}' (replace mode).")
        df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False, schema="datasf")
        logger.info("Data successfully loaded into Postgres.")

    @task()
    def create_indexes():
        logger = logging.getLogger("airflow.task")
        logger.info("Creating indexes on datasf.fire_incidents table")

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Define indexes to create - each tuple contains (name, columns, type)
        indexes = [
            ("idx_fire_incident_number", ["incident_number"], ""),
            ("idx_fire_incident_date", ["incident_date"], ""),
        ]

        # Add primary key constraint to 'id' column if not exists
        add_pk_sql = f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
            SELECT 1
            FROM information_schema.table_constraints
            WHERE table_schema = 'datasf'
              AND table_name = '{TABLE_NAME}'
              AND constraint_type = 'PRIMARY KEY'
            ) THEN
            ALTER TABLE datasf.{TABLE_NAME}
            ADD CONSTRAINT {TABLE_NAME}_pkey PRIMARY KEY (id);
            END IF;
        END
        $$;
        """
        logger.info("Ensuring primary key constraint on 'id' column...")
        pg_hook.run(add_pk_sql)

        for idx_name, columns, idx_type in indexes:
            columns_str = ", ".join([f'"{col}"' for col in columns])
            create_idx_sql = f"""
            CREATE INDEX IF NOT EXISTS {idx_name}
            ON datasf.{TABLE_NAME} ({columns_str});
            """
            logger.info(f"Creating index {idx_name}...")
            pg_hook.run(create_idx_sql)

        logger.info("All indexes and constraints created successfully")

    create_schema_task = create_schema()
    create_table_task = create_table()
    csv_file = download_csv()
    load_task = load_to_postgres(csv_file)
    indexes_task = create_indexes()

    create_schema_task >> create_table_task >> csv_file >> load_task >> indexes_task


# Instantiate the DAG
initial_loader()
