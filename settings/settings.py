from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    csv_file_path: str
    db_connection_uri: str
    temperature_unit: str
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")
