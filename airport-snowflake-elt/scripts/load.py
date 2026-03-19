import boto3
from io import StringIO
from scripts.extract import extract_data

def run_pipeline():
    df1=extract_data()
    # S3 client
    s3 = boto3.client(
        's3',
        aws_access_key_id='******',
        aws_secret_access_key='********'
        )

    # converting dataframe into csv
    csv_buffer = StringIO()
    df1.to_csv(csv_buffer, index=False)

    # uploading to S3
    bucket_name = 'airport-raw-datawal'
    file_name_in_s3 = 'data/airline_data.csv'

    s3.put_object(Bucket=bucket_name, Key=file_name_in_s3, Body=csv_buffer.getvalue())

