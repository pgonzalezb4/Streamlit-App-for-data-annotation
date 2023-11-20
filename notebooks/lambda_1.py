# concatenate-raw-labeled-news-data

import json
import boto3
import logging
import urllib.parse
import pandas as pd

from io import BytesIO
from http import HTTPStatus

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')


def lambda_handler(event, context):
    
    s3_client = boto3.client('s3')

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    username = key.split('/')[0]
    
    bucket_objs = s3_client.list_objects_v2(Bucket=bucket, Prefix=f"{username}/")
    
    dfs = [pd.read_parquet(BytesIO(s3_client.get_object(Bucket=bucket, Key=elem['Key'])['Body'].read())) for elem in bucket_objs['Contents']]
    
    processed_labeled_data = pd.concat(dfs).drop_duplicates(subset=['article_id']).reset_index(drop=True)
    
    out_buffer = BytesIO()
    processed_labeled_data.to_parquet(out_buffer, index=False, compression='gzip')
    
    s3_client.put_object(Body=out_buffer.getvalue(),
                             Bucket='processed-labeled-news-data',
                             Key=f'{username}/processed_labeled_dataset.parquet.gzip')
    
    return {
        "statusCode": HTTPStatus.OK,
        "headers": {"content-type": "application/json"},
        "body": {"rows_procesed": len(processed_labeled_data)}
    }

