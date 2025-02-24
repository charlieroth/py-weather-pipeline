from prefect import task
import polars as pl
import os


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

    Output: DataFrame containing the raw weather data.
    """
    try:
        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"File not found: {csv_file_path}")

        df = pl.read_csv(csv_file_path, null_values=["null"])

        if not contains_expected_columns(df):
            raise ValueError("Column validation failed")

        if not valid_date_range(df["dt_iso"]):
            raise ValueError("Date range validation failed")

        if not valid_kelvin_temperatures(df["temp"]):
            raise ValueError("Temperature validation failed")

        return df
    except FileNotFoundError:
        print(f"File not found: {csv_file_path}")
    except ValueError as e:
        print(f"Data validation failed: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


def contains_expected_columns(df: pl.DataFrame) -> bool:
    """
    Check if the DataFrame contains the expected columns
    """
    expected_columns = [
        "dt",
        "dt_iso",
        "timezone",
        "city_name",
        "lat",
        "lon",
        "temp",
        "visibility",
        "dew_point",
        "feels_like",
        "temp_min",
        "temp_max",
        "pressure",
        "sea_level",
        "grnd_level",
        "humidity",
        "wind_speed",
        "wind_deg",
        "wind_gust",
        "rain_1h",
        "rain_3h",
        "snow_1h",
        "snow_3h",
        "clouds_all",
        "weather_id",
        "weather_main",
        "weather_description",
        "weather_icon",
    ]
    return all(col in df.columns for col in expected_columns)


def valid_date_range(dt_iso_series: pl.Series) -> bool:
    """
    Check if the `dt_iso` column values are within the expected date range
    """
    # Convert dt_iso to datetime type
    parsed_dates = dt_iso_series.str.extract(r"^([\d-]+\s[\d:]+)").str.to_datetime(
        "%Y-%m-%d %H:%M:%S"
    )

    # Define the date range boundaries
    start_date = pl.datetime(2015, 1, 1, 0, 0, 0)
    end_date = pl.datetime(2025, 1, 1, 23, 59, 59)

    # Check if all dates are within the expected range
    dates_in_range = parsed_dates.filter(parsed_dates.is_between(start_date, end_date))

    # Count records in range vs total records
    total_records = len(parsed_dates)
    records_in_range = len(dates_in_range)
    return total_records == records_in_range


def valid_kelvin_temperatures(temp_series: pl.Series) -> bool:
    """
    Check if the `temp` column values are within the expected temperature range

    The theoretical minimum Kelvin temperature is `0`.
    A practical minimum is `180` (coldest temperature ever recorded on Earth, with some buffer).
    A practical maximum is `340` (hotest temperature ever recorded on Earth, with some buffer).
    """
    # Define the temperature range boundaries
    min_temp = 180
    max_temp = 340

    # Check if all temperatures are within the expected range
    temps_in_range = temp_series.filter(temp_series.is_between(min_temp, max_temp))
    return len(temps_in_range) == len(temp_series)
