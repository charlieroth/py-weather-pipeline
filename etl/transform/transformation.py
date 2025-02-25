import polars as pl


def transform_temperature_columns(
    df: pl.DataFrame, columns: list[str], temperature_unit: str
) -> pl.DataFrame:
    for column in columns:
        if temperature_unit == "celsius":
            df = kelvin_to_celsius(df, column)
        elif temperature_unit == "farenheit":
            df = kelvin_to_farenheit(df, column)

    return df


def kelvin_to_celsius(df: pl.DataFrame, column_name: str) -> pl.DataFrame:
    return df.with_columns((pl.col(column_name) - 273.15).alias(column_name))


def kelvin_to_farenheit(df: pl.DataFrame, column_name: str) -> pl.DataFrame:
    return df.with_columns((pl.col(column_name) - 273.15) * 9 / 5 + 32).alias(
        column_name
    )


def add_temporal_features(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            pl.col("dt_iso").dt.year().alias("year"),
            pl.col("dt_iso").dt.month().alias("month"),
            pl.col("dt_iso").dt.day().alias("day"),
            pl.col("dt_iso").dt.hour().alias("hour"),
            pl.col("dt_iso").dt.weekday().alias("day_of_week"),
            pl.col("dt_iso").dt.quarter().alias("quarter"),
            # is weekend flag
            (pl.col("dt_iso").dt.weekday() >= 5).alias("is_weekend"),
            # Season (Northern Hemisphere)
            pl.when(pl.col("dt_iso").dt.month().is_in([12, 1, 2]))
            .then("winter")
            .when(pl.col("dt_iso").dt.month().is_in([3, 4, 5]))
            .then("spring")
            .when(pl.col("dt_iso").dt.month().is_in([6, 7, 8]))
            .then("summer")
            .otherwise("fall")
            .alias("season"),
        ]
    )


def add_weather_condition_categories(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        # weather type categorization based on values from `weather_main` column
        [
            pl.when(pl.col("weather_main") == "Clear")
            .then("clear")
            .when(pl.col("weather_main") == "Clouds")
            .then("cloudy")
            .when(pl.col("weather_main").is_in(["Rain", "Drizzle"]))
            .then("rainy")
            .when(pl.col("weather_main") == "Snow")
            .then("snowy")
            .when(pl.col("weather_main") == "Thunderstorm")
            .then("stormy")
            .when(pl.col("weather_main").is_in(["Mist", "Fog", "Haze", "Smoke"]))
            .then("poor_visibility")
            .when(pl.col("weather_main") == "Squall")
            .then("windy")
            .otherwise("other")
            .alias("weather_condition_category"),
            # more detailed categorization for specific analysis needs
            pl.when(pl.col("weather_main") == "Clear")
            .then("clear")
            .when(pl.col("weather_main") == "Clouds")
            .then("cloudy")
            .when(pl.col("weather_main") == "Rain")
            .then("rain")
            .when(pl.col("weather_main") == "Drizzle")
            .then("drizzle")
            .when(pl.col("weather_main") == "Snow")
            .then("snow")
            .when(pl.col("weather_main") == "Thunderstorm")
            .then("thunderstorm")
            .when(pl.col("weather_main") == "Mist")
            .then("mist")
            .when(pl.col("weather_main") == "Fog")
            .then("fog")
            .when(pl.col("weather_main") == "Haze")
            .then("haze")
            .when(pl.col("weather_main") == "Smoke")
            .then("smoke")
            .when(pl.col("weather_main") == "Squall")
            .then("squall")
            .otherwise("other")
            .alias("weather_detailed"),
            # binary condition flags
            (pl.col("weather_main") == "Clear").alias("is_clear"),
            (pl.col("weather_main") == "Clouds").alias("is_cloudy"),
            (pl.col("weather_main").is_in(["Rain", "Drizzle"])).alias("is_rainy"),
            (pl.col("weather_main") == "Snow").alias("is_snowy"),
            (pl.col("weather_main") == "Thunderstorm").alias("is_stormy"),
            (pl.col("weather_main").is_in(["Mist", "Fog", "Haze", "Smoke"])).alias(
                "poor_visibility"
            ),
            # percipitation flags (based on weather condition and percipitation data)
            (
                pl.col("weather_main").is_in(
                    ["Rain", "Drizzle", "Snow", "Thunderstorm"]
                )
                | pl.col("rain_1h").is_not_null()
                | pl.col("snow_1h").is_not_null()
            ).alias("has_percipitation"),
            # severity level (general weather condition severity)
            pl.when(pl.col("weather_main").is_in(["Clear", "Clouds"]))
            .then(0)  # normal conditions
            .when(pl.col("weather_main").is_in(["Mist", "Haze", "Smoke"]))
            .then(1)  # minor impact
            .when(pl.col("weather_main").is_in(["Fog", "Drizzle"]))
            .then(2)  # moderate impact
            .when(pl.col("weather_main").is_in(["Rain", "Snow", "Squall"]))
            .then(3)  # significant impact
            .when(pl.col("weather_main") == "Thunderstorm")
            .then(4)  # severe impact
            .otherwise(0)
            .alias("severity_level"),
        ]
    )

    # enhance weather features combining multiple data points
    return df.with_columns(
        [
            # combine cloud cover with weather conditions
            pl.when((pl.col("weather_main") == "Clouds") & (pl.col("clouds_all") <= 30))
            .then("partly_cloud")
            .when(
                (pl.col("weather_main") == "Clouds")
                & (pl.col("clouds_all") > 30)
                & (pl.col("clouds_all") <= 70)
            )
            .then("most_cloudy")
            .when((pl.col("weather_main") == "Clouds") & (pl.col("clouds_all") > 70))
            .then("overcast")
            .otherwise(pl.col("weather_main"))
            .alias("cloud_detail"),
            # combine rain condition with intensity
            pl.when(
                (pl.col("weather_main").is_in(["Rain", "Drizzle"]))
                & (pl.col("rain_1h").is_null() | (pl.col("rain_1h") < 0.5))
            )
            .then("light_rain")
            .when(
                (pl.col("weather_main").is_in(["Rain", "Drizzle"]))
                & (pl.col("rain_1h") >= 0.5)
                & (pl.col("rain_1h") < 4.0)
            )
            .then("moderate_rain")
            .when(
                (pl.col("weather_main").is_in(["Rain", "Drizzle"]))
                & (pl.col("rain_1h") >= 4.0)
            )
            .then("heavy_rain")
            .otherwise(None)
            .alias("rain_intensity"),
            # combine snow condition with intensity
            pl.when(
                (pl.col("weather_main") == "Snow")
                & (pl.col("snow_1h").is_null() | (pl.col("snow_1h") < 0.5))
            )
            .then("light_snow")
            .when(
                (pl.col("weather_main") == "Snow")
                & (pl.col("snow_1h") >= 0.5)
                & (pl.col("snow_1h") < 4.0)
            )
            .then("moderate_snow")
            .when((pl.col("weather_main") == "Snow") & (pl.col("snow_1h") >= 4.0))
            .then("heavy_snow")
            .otherwise(None)
            .alias("snow_intensity"),
            # visibility impact from weather
            pl.when(pl.col("weather_main").is_in(["Mist", "Haze"]))
            .then("slightly_reduced")
            .when(pl.col("weather_main").is_in(["Fog", "Smoke"]))
            .then("significantly_reduced")
            .when(pl.col("weather_main") == "Clear")
            .then("excellent")
            .otherwise("moderate")
            .alias("visibility_impact"),
            # weather condition seasonality (useful for time series analysis)
            (
                pl.col("weather_main").is_in(["Snow", "Fog"])
                & pl.col("dt_iso").dt.month().is_in([12, 1, 2, 3])
            ).alias("typical_winter_condition"),
            (
                pl.col("weather_main")
                == "Thunderstorm" & pl.col("dt_iso").dt.month().is_in([6, 7, 8])
            ).alias("typical_summer_condition"),
        ]
    )


def add_wind_features(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            # convert wind direction to cardinal direction
            pl.when(pl.col("wind_deg") >= 337.5 | pl.col("wind_deg") < 22.5)
            .then("N")
            .when(pl.col("wind_deg") >= 22.5 & pl.col("wind_deg") < 67.5)
            .then("NE")
            .when(pl.col("wind_deg") >= 67.5 & pl.col("wind_deg") < 112.5)
            .then("E")
            .when(pl.col("wind_deg") >= 112.5 & pl.col("wind_deg") < 157.5)
            .then("SE")
            .when(pl.col("wind_deg") >= 157.5 & pl.col("wind_deg") < 202.5)
            .then("S")
            .when(pl.col("wind_deg") >= 202.5 & pl.col("wind_deg") < 247.5)
            .then("SW")
            .when(pl.col("wind_deg") >= 247.5 & pl.col("wind_deg") < 292.5)
            .then("W")
            .otherwise("NW")
            .alias("wind_direction"),
            # wind speed categorization
            pl.when(pl.col("wind_speed") < 0.5)
            .then("calm")
            .when(pl.col("wind_speed") < 3.3)
            .then("light")
            .when(pl.col("wind_speed") < 7.9)
            .then("moderate")
            .when(pl.col("wind_speed") < 13.8)
            .then("fresh")
            .when(pl.col("wind_speed") < 20.7)
            .then("strong")
            .otherwise("storm")
            .alias("wind_category"),
            # wind gust ratio (when available)
            (pl.col("wind_gust") / pl.col("wind_speed")).alias("gust_ratio"),
        ]
    )


def add_temperature_related_features(
    df: pl.DataFrame, temperature_unit: str = "kelvin"
) -> pl.DataFrame:
    def kelvin_to_celsius(col):
        return col - 273.15

    def farenheit_to_celsius(col):
        return (col - 32) * 5 / 9

    def identity(col):
        return col

    if temperature_unit.lower() == "kelvin":
        # convert kelvin to celsius
        to_celsius = kelvin_to_celsius
        # original temperature is already in kelvin
        temp_k = pl.col("temp")
    elif temperature_unit.lower() == "celsius":
        # already in celsius
        to_celsius = identity
        # original temperature is already in celsius
        temp_k = pl.col("temp")
    elif temperature_unit.lower() == "farenheit":
        # convert farenheit to celsius
        to_celsius = farenheit_to_celsius
        # convert farenheit to kelvin
        temp_k = (pl.col("temp") - 32) * (5 / 9) + 273.15
    else:
        raise ValueError(f"Invalid temperature unit: {temperature_unit}")

    return df.with_columns(
        [
            # temperature comfort index (simplified)
            (to_celsius(pl.col("temp")) - 0.55 * (1 - pl.col("humidity") / 100))
            * (to_celsius(pl.col("temp")) - 14.5).alias("comfort_index"),
            # heat index (when temp > 27C)
            pl.when(to_celsius(pl.col("temp")) > 27)
            .then(
                pl.lit(-8.7846947556)
                + pl.lit(1.61139411) * to_celsius(pl.col("temp"))
                + pl.lit(2.33854883889) * pl.col("humidity") / 100
                - pl.lit(0.14611605)
                * to_celsius(pl.col("temp"))
                * pl.col("humidity")
                / 100
            )
            .otherwise(to_celsius(pl.col("temp")))
            .alias("heat_index"),
            # dew point depression (difference between temp and dew point)
            # ensure bother are in the same unit before subtraction
            (temp_k - pl.col("dew_point")).alias("dew_point_depression"),
            # apparent temperature difference
            (pl.col("feels_like") - pl.col("temp")).alias("apparent_temp_diff"),
            # wind chill (for cold temperatures)
            pl.when(to_celsius(pl.col("temp")) < 10)
            .then(
                13.12
                + 0.6215 * to_celsius(pl.col("temp"))
                - 11.37 * pl.col("wind_speed").pow(0.16)
                + 0.3965 * to_celsius(pl.col("temp")) * pl.col("wind_speed").pow(0.16)
            )
            .otherwise(to_celsius(pl.col("temp")))
            .alias("wind_chill"),
            # temperature range for measurement
            (pl.col("temp_max") - pl.col("temp_min")).alias("temp_range"),
            # humidex (canadian humidity index)
            (
                to_celsius(pl.col("temp"))
                + 0.5555
                * (
                    6.11 * pl.exp(5417.7530 * (1 / 273.16 - 1 / (pl.col("dew_point"))))
                    - 10
                )
            ).alias("humidex"),
        ]
    )


def add_pressure_tendency_features(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            # 3-hour pressure change
            (pl.col("pressure") - pl.col("pressure").shift(3)).alias(
                "pressure_change_3h"
            ),
            # pressure tendency categorization
            pl.when((pl.col("pressure") - pl.col("pressure").shift(3)) > 1)
            .then("rising")
            .when((pl.col("pressure") - pl.col("pressure").shift(3)) < -1)
            .then("falling")
            .otherwise("steady")
            .alias("pressure_tendency"),
        ]
    )


def add_cloud_cover_and_visibility_features(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            # visibility categorization
            pl.when(pl.col("visibility") < 1000)
            .then("very_poor")
            .when(pl.col("visibility") < 4000)
            .then("poor")
            .when(pl.col("visibility") < 10000)
            .then("moderate")
            .when(pl.col("visibility") < 20000)
            .then("good")
            .otherwise("excellent")
            .alias("visibility_category"),
            # cloud cover categorization
            pl.when(pl.col("clouds_all") <= 10)
            .then("clear")
            .when(pl.col("clouds_all") <= 30)
            .then("mostly_clear")
            .when(pl.col("clouds_all") <= 70)
            .then("partly_cloudy")
            .when(pl.col("clouds_all") <= 90)
            .then("mostly_cloudy")
            .otherwise("cloudy")
            .alias("cloud_cover_category"),
        ]
    )


def add_temperature_difference(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("temp_max") - pl.col("temp_min")).alias("temp_difference")
    )
