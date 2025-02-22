from prefect import task
import polars as pl


@task(log_prints=True)
def transform_weather_data(
    raw_weather_df: pl.DataFrame,
    temperature_unit: str,
) -> pl.DataFrame:
    """
    Goal: Clean, transform and prepare the raw weather data for loading into PostgreSQL.

    Task:

    - Data Cleaning
        - Missing Value Handling: Check for missing values (`NaN`, `None`) in DataFrame
            - Decide on strategy
                - Imputation: Fill missing values using methods like mean, median, or forward/backward fill
                - Removal: If missing values are insignificant, might choose to drop rows or columns
        - Data Type Conversion: Ensure all columns have correct data types
            - Convert timestamp columns to `datetime` objects if they are not already
        - Handling Inconsistencies (if any): Look for any data inconsistencies
            - E.g., negative humidity values
    - Data Transformation
        - Unit Conversion: Convert from Kelvin to Celsius and/or Farenheit
        - Feature Engineering: Create new features from existing ones to enrich your data
            - Temperature Difference: Calculate the difference between max and min temperatures for each day
            - Datetime Components: Extract day of the week, month, year, season from the date
            - Weather Condition Categorization: Group weather condition descriptions into broader categories (e.g., 'Clear', 'Rainy', 'Cloudy', 'Snowy')
    - Data Validation
        - Range Checks: Ensure temperature values in Celsius/Farenheit are within expected ranges
        - Data Consistency: Verify logical consistency between features (e.g., if 'rain' is 0, 'weather condition' shouldn't be 'Heavy Rain')

    Output: Transformed and Validated Polars DataFrame
    """
    try:
        return pl.DataFrame()
    except Exception as e:
        raise e
