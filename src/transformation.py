import polars as pl


def transform_earthquake_data(df: pl.DataFrame) -> pl.DataFrame:

    province_col = (pl.col("LOCATION").str.extract(r"\(([^)]+)\)", 1)).alias("PROVINCE")

    magnitude_class_col = (
        pl.col("MAG")
        .cast(pl.Float64)
        .cut(
            breaks=[3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
            labels=["Micro", "Minor", "Light", "Moderate", "Strong", "Major", "Great"],
        )
        .alias("MAGNITUDE_CLASS")
    )

    depth_cast_col = pl.col("DEPTH").cast(pl.Int32)

    depth_class_col = (
        pl.col("DEPTH")
        .cast(pl.Int32)
        .cut(breaks=[70, 300], labels=["Shallow", "Intermediate", "Deep"])
        .alias("DEPTH_CLASS")
    )

    transformed_df = df.with_columns(
        [province_col, magnitude_class_col, depth_cast_col, depth_class_col]
    )

    return transformed_df
