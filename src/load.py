import io
import os

import boto3
import polars as pl
from dotenv import load_dotenv

load_dotenv()


def load_to_s3(df: pl.DataFrame):
    bucket_name = os.getenv("BUCKET_NAME")
    region_name = os.getenv("REGION_NAME")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    s3_client = boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
    )

    first_date = df.get_column("DATE")[0]
    year = first_date.year
    month = f"{first_date.month:02d}"
    file_name = f"{first_date}.parquet"

    s3_key = f"processed/year={year}/month={month}/{file_name}"

    buffer = io.BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)

    s3_client.upload_fileobj(buffer, bucket_name, s3_key)

    return f"s3://{bucket_name}/{s3_key}"
    return f"s3://{bucket_name}/{s3_key}"
    return f"s3://{bucket_name}/{s3_key}"
