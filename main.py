from etl.flow import weather_etl_flow
from settings.settings import Settings

if __name__ == "__main__":
    settings = Settings()
    weather_etl_flow.serve(
        name="weather-etl",
        tags=["weather-etl"],
        parameters={
            "csv_file_path": settings.csv_file_path,
            "db_connection_uri": settings.db_connection_uri,
            "temperature_unit": settings.temperature_unit,
        },
    )
