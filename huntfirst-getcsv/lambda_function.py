import json
import urllib.parse
import boto3
from parse import parse

s3 = boto3.resource('s3')

def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
        parse(s3, bucket, key)
        
        print('success!')
    except Exception as e:
        print(e)
        raise e
