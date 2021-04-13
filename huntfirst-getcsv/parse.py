"""
Script for parsing out specific huntcodes from csv
"""
import csv
import boto3

def check_hunt_code(path, hunt_code):
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        line_count = 0
        dynamodb = boto3.resource('dynamodb')

        table = dynamodb.Table('huntcodes')
        
        for row in reader:
            if line_count == 0:
                line_count += 1
                continue

            if row["HUNT CODE "].strip().lower() == hunt_code.lower():
                return True
            
            line_count += 1 

def parse(s3, bucket, key):
    path = '/tmp/' + key[7:]
    s3.Bucket(bucket).download_file(key, path)
    if check_hunt_code(path, 'EF181P5R'):
        print('found it!')