from prefect import task
import polars as pl


@task(log_prints=True)
def extract_weather_data(csv_file_path: str) -> pl.DataFrame:
    """
    Goal: Read the weather data from the CSV file.

    Task:
        - Use polars to read the CSV file into a DataFrame.
        - Data Validation
        - Schema Validation
            - Ensure expected columns are present.
        - Data Type Validation
            - Check if columns have the expected data types (e.g., 'temperature' is numeric, 'timestamp' is datetime-like).
        - Basic Data Range Checks
            - Verify dates are within expected ranges (2015-01-01 to 2025-01-01)
            - Temperatures are within plausible ranges (even in Kelvin)
        - Error Handling
            - Raise an error if the CSV file is not found.
            - Raise an error if CSV file is corrupted.
            - Raise an error on data validation errors.
            - Use prefect's error handling mechanisms to gracefully manage these situations (e.g., logging errors, retries)

    Output: DataFrame containing the raw weather data.
    """
    try:
        return pl.DataFrame()
    except Exception as e:
        raise e
