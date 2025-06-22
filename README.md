# Fire Project

A data engineering project built with [Apache Airflow](https://airflow.apache.org/) and [dbt](https://www.getdbt.com/), containerized using Docker.

## Requirements

- [Astronomer](https://www.astronomer.io/)
- [Docker](https://www.docker.com/)

## Getting Started

1. **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd fire
    ```

2. **Install Docker:**

    If you don't have Docker installed, follow the instructions for your operating system:

    - [Docker for Mac](https://docs.docker.com/desktop/install/mac-install/)
    - [Docker for Windows](https://docs.docker.com/desktop/install/windows-install/)
    - [Docker for Linux](https://docs.docker.com/engine/install/)

3. **Install the Astro CLI (if not already installed):**

    The Astro CLI is required for managing and deploying Astro projects. Install it globally using npm:

    For more installation options and details, refer to the [Astro CLI installation guide](https://www.astronomer.io/docs/astro/cli/install-cli/).

4. **Start the development server with Astro:**
    ```bash
    astro dev start
    ```

## Dags
### datasf_fire_incidents_initial_loader
#### Description

The `initial_loader.py` DAG is responsible for the initial ingestion of fire incident data from the DataSF export CSV into the data warehouse. It extracts raw data, performs basic validation and transformation, and loads it into the staging tables for downstream processing.

#### Usage

This DAG should be triggered once to perform the initial load of historical fire incident data. Subsequent updates should use the incremental loader DAG.

#### Index Performance Strategy

The initial loader implements a performance-optimized indexing strategy:
- Indexes are created **after** bulk data loading to improve initial load performance
- Key indexes include:
  - `incident_number`: Primary identifier for fire incidents, optimizes lookups and joins
  - `incident_date`: Accelerates time-based filtering and analysis
- This strategy balances query performance against load time by:
  1. Loading all data efficiently in a single transaction without index overhead
  2. Creating targeted indexes after load completion to optimize subsequent queries
  3. Focusing on columns most frequently used in filtering and join operations

#### Example

To trigger the DAG manually from the Airflow UI, select `datasf_fire_incidents_initial_loader` and click "Trigger DAG".

![Screenshot of run_dbt DAG](/assets/datasf_fire_incidents_initial_loader-graph.png)
### datasf_fire_incidents_incremental_load
#### Description

The `incremental_load.py` DAG handles the regular ingestion of new or updated fire incident records from the DataSF API. It is designed to run on a schedule, fetching only data that has changed since the last 2 days, and updating the data warehouse accordingly.

#### Merge & Deduplication Strategy

The incremental loader implements a robust merge and deduplication strategy to ensure data consistency and accuracy:

1. **Time-based Incremental Loading**:
   - Uses Airflow's logical_date (execution_date) to determine the appropriate look-back period
   - Queries the Socrata API for records with `data_as_of` timestamp greater than or equal to 2 days before the execution date
   - This overlap window ensures no data is missed due to processing delays or timezone differences

2. **Upsert Mechanism**:
   - Implements a database-level upsert (INSERT ... ON CONFLICT) operation using PostgreSQL's native capabilities
   - Uses the unique `id` field as the conflict identifier to detect duplicates
   - When a duplicate is detected, the existing record is completely replaced with the new data
   - This approach handles both new records and updates to existing records in a single transaction

3. **Late-arriving Data Handling**:
   - The 2-day lookback window accommodates late-arriving data
   - Records updated after initial ingestion are automatically captured in subsequent runs
   - Historical corrections and amendments are properly integrated without manual intervention

4. **Transaction Safety**:
   - All database operations are wrapped in a transaction
   - Automatic rollback occurs if any part of the process fails
   - Ensures database consistency even during partial failures

5. **Batch Processing**:
   - Data is fetched and processed in batches of 1,000 records
   - Improves memory efficiency and allows processing of large datasets
   - Records are committed every 1,000 rows to balance transaction size with performance

This strategy provides a reliable method for keeping the fire incidents dataset current while handling edge cases such as data corrections, delayed updates, and ensuring no duplicate records exist in the final dataset.

#### Usage

This DAG should be scheduled to run daily to keep the fire incident data up to date. It ensures that only new or modified records are processed, optimizing resource usage and minimizing load times.

#### Example

To trigger the DAG manually from the Airflow UI, select `datasf_fire_incidents_incremental_load` and click "Trigger DAG". For automated operation, configure a schedule interval in the DAG definition.

![Screenshot of run_dbt DAG](/assets/datasf_fire_incidents_incremental_load-graph.png)
### datasf_fire_incidents_run_dbt
#### Description

The `run_dbt.py` DAG is responsible for orchestrating dbt (data build tool) runs within the Airflow environment. It executes dbt models and tests as part of the data pipeline, ensuring that transformations and data quality checks are applied to the ingested fire incident data.

#### Usage

This DAG can be triggered manually or scheduled to run after data ingestion DAGs complete. It ensures that the latest data is transformed and validated according to the dbt project configuration.

#### Example

To trigger the DAG manually from the Airflow UI, select `datasf_fire_incidents_run_dbt` and click "Trigger DAG". For automated workflows, set up dependencies so this DAG runs after data loading DAGs.

![Screenshot of run_dbt DAG](/assets/datasf_fire_incidents_run_dbt-graph.png)