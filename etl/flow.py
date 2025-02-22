from prefect import flow
from etl.extract.task import extract_weather_data
from etl.transform.task import transform_weather_data
from etl.load.task import load_weather_data_to_postgres


@flow(log_prints=True)
def weather_etl_flow(csv_file_path: str, db_connection_uri: str, temperature_unit: str):
    try:
        raw_weather_df = extract_weather_data(csv_file_path)
        transformed_weather_df = transform_weather_data(
            raw_weather_df, temperature_unit
        )
        load_result = load_weather_data_to_postgres(
            transformed_weather_df, db_connection_uri
        )
        print("Weather ETL Flow Completed with result: ", load_result)
    except Exception as e:
        raise e
