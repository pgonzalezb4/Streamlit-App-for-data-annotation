# get-unlabeled-data

import json
import boto3
import logging
import pandas as pd

from io import BytesIO
from http import HTTPStatus

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    
    username = json.loads(event["body"])["username"]
    logging.info(f'Retrieving data for username {username}')
    
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket='unlabeled-news-data', Key=f'{username}/unlabeled_dataset.parquet.gzip')
    unlabeled_data = pd.read_parquet(BytesIO(response['Body'].read()))
    
    try:
        response = s3_client.get_object(Bucket='processed-labeled-news-data', Key=f'{username}/processed_labeled_dataset.parquet.gzip')
        labeled_data = pd.read_parquet(BytesIO(response['Body'].read()))
    except:
        data_to_label = unlabeled_data.copy()
    else:
        labeled_articleids = labeled_data.article_id.unique()
        data_to_label = unlabeled_data[~unlabeled_data.article_id.isin(labeled_articleids)].copy()
    
    return {
        "statusCode": HTTPStatus.OK,
        "headers": {"content-type": "application/json"},
        "body": data_to_label.to_json()
    }

    
    
    
