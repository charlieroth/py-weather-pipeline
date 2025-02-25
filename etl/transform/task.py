from prefect import task
import polars as pl

from etl.transform.cleaning import (
    convert_to_datetime,
    convert_to_float,
    drop_columns_with_missing_values,
)
from etl.transform.transformation import (
    add_cloud_cover_and_visibility_features,
    add_pressure_tendency_features,
    add_temperature_difference,
    add_temporal_features,
    add_weather_condition_categories,
    add_wind_features,
    add_temperature_related_features,
    transform_temperature_columns,
)


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
        # === Data Cleaning ===
        # --- Missing Value Handling ---
        # drop columns with all null values
        df = drop_columns_with_missing_values(raw_weather_df)
        # --- Data Type Conversion ---
        # convert `dt_iso` to datetime, represented as string in the CSV
        df = convert_to_datetime(raw_weather_df, "dt_iso")
        # convert `rain_1h`, `rain_3h`, `snow_1h`, `snow_3h` to float, represented as strings in the CSV
        df = convert_to_float(df, ["rain_1h", "rain_3h", "snow_1h", "snow_3h"])

        # === Data Transformation ===
        # --- Unit Conversion ---
        # convert `dew_point`, `feels_like`, `temp`, `temp_min`, `temp_max` to Celsius or Farenheit (based on `temperature_unit`)
        df = transform_temperature_columns(
            df,
            ["dew_point", "feels_like", "temp", "temp_min", "temp_max"],
            temperature_unit,
        )
        # --- Feature Engineering ---
        df = add_temperature_difference(df)
        df = add_temporal_features(df)
        df = add_weather_condition_categories(df)
        df = add_wind_features(df)
        df = add_temperature_related_features(df, temperature_unit)
        df = add_pressure_tendency_features(df)
        df = add_cloud_cover_and_visibility_features(df)

        # === Data Validation ===
        # --- Range Checks ---
        # ...
        # --- Data Consistency Checks ---
        # ...
        return df
    except Exception as e:
        raise e
