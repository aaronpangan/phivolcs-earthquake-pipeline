import polars as pl


def transform_earthquake_data(df: pl.DataFrame) -> pl.DataFrame:

    province_col = (pl.col("LOCATION").str.extract(r"\(([^)]+)\)", 1)).alias("PROVINCE")

    magnitude_class_col = (
        pl.col("MAG")
        .cast(pl.Float64, strict=False)
        .cut(
            breaks=[3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
            labels=["Micro", "Minor", "Light", "Moderate", "Strong", "Major", "Great"],
        )
        .alias("MAGNITUDE_CLASS")
    )

    clean_depth_col = (
        pl.col("DEPTH")
        .str.replace_all(r"[^0-9.]", "") 
        .cast(pl.Int32, strict=False)  
        .alias("DEPTH")
    )

    depth_class_col = (
        pl.col("DEPTH")
        .str.replace_all(r"[^0-9.]", "")
        .cast(pl.Int32, strict=False)
        .cut(breaks=[70, 300], labels=["Shallow", "Intermediate", "Deep"])
        .alias("DEPTH_CLASS")
    )

    datetime_parsed = pl.col("DATE_TIME").str.strptime(
        pl.Datetime, "%d %B %Y - %I:%M %p", strict=False
    )

    date_col = datetime_parsed.dt.date().alias("DATE")
    time_col = datetime_parsed.dt.strftime("%I:%M %p").alias("TIME")
    am_pm_col = datetime_parsed.dt.strftime("%p").alias("TIME_PERIOD")

    transformed_df = df.with_columns(
        [
            province_col,
            magnitude_class_col,
            clean_depth_col,
            depth_class_col,
            date_col,
            time_col,
            am_pm_col,
        ]
    )

    return transformed_df
