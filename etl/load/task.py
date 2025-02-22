from prefect import task
import polars as pl


@task(log_prints=True)
def load_weather_data_to_postgres(
    transformed_weather_df: pl.DataFrame,
    db_connection_uri: str,
) -> None:
    """
    Goal: Load the transformed and cleaned weather data into PostgreSQL.

    Task:

    - Database Connection: Establish connection to PostgreSQL database
    - Schema Creation
        - Table Creation (if not exists): Implement logic to create the necessary table(s) in PostgreSQL database if they don't exist.
          Define your table schmema based on the transformed data and your analysis needs. Consider:
            - Data types in PostgreSQL that map to your Polars DataFrame columns
            - Choosing primary key (e.g., a combination of data and location, or just date if it's for a single location and daily granularity)
        - Schema Evolution:

    """
    try:
        return None
    except Exception as e:
        raise e
