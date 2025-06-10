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

#### Example

To trigger the DAG manually from the Airflow UI, select `datasf_fire_incidents_initial_loader` and click "Trigger DAG".

![Screenshot of run_dbt DAG](/assets/datasf_fire_incidents_initial_loader-graph.png)
### datasf_fire_incidents_incremental_load
#### Description

The `incremental_load.py` DAG handles the regular ingestion of new or updated fire incident records from the DataSF API. It is designed to run on a schedule, fetching only data that has changed since the last 2 days, and updating the data warehouse accordingly.

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