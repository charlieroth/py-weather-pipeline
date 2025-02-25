import polars as pl


def drop_columns_with_missing_values(df: pl.DataFrame) -> pl.DataFrame:
    columns = df.columns()
    all_null_columns = []
    for column in columns:
        if is_all_null(df, column):
            all_null_columns.append(column)

    return df.drop(all_null_columns)


def is_all_null(df: pl.DataFrame, column: str) -> bool:
    non_null_count = df.select(pl.col(column).is_not_null().sum()).item()
    return non_null_count == 0


def convert_to_datetime(df: pl.DataFrame, column_name: str) -> pl.DataFrame:
    df = df.with_columns(
        pl.col(column_name).str.replace(" \\+0000 UTC$", "").alias(column_name)
    )

    df = df.with_columns(
        pl.col(column_name)
        .str.strptime(dtype=pl.Datetime, format="%Y-%m-%d %H:%M:%S")
        .alias(column_name)
    )

    return df


def convert_to_float(df: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    for column in columns:
        df = df.with_columns(
            pl.col(column).cast(pl.Float64, strict=False).alias(column)
        )
    return df
