import json
import urllib.parse
import boto3
from textract import run_textract


def lambda_handler(event, context):

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:

        run_textract(bucket, key)
        
        print("Success!")
        
    
    except Exception as e:
        print(e)
        print('Error processing object {} from bucket {}.'.format(key, bucket))
        raise e
