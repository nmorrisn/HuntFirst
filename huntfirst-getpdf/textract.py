import boto3
import time
import os

from datetime import datetime
from botocore.errorfactory import ClientError

def startJob(s3BucketName, objectName):
    response = None
    client = boto3.client('textract')
    response = client.start_document_analysis(
    DocumentLocation={
        'S3Object': {
            'Bucket': s3BucketName,
            'Name': objectName
        }
    },
    FeatureTypes=['TABLES'])

    return response["JobId"]

def isJobComplete(jobId):
    time.sleep(5)
    client = boto3.client('textract')
    response = client.get_document_analysis(JobId=jobId)
    status = response["JobStatus"]
    print("Job status: {}".format(status))

    while(status == "IN_PROGRESS"):
        time.sleep(5)
        response = client.get_document_analysis(JobId=jobId)
        status = response["JobStatus"]
        print("Job status: {}".format(status))

    return status

def getJobResults(jobId):

    pages = []

    time.sleep(5)

    client = boto3.client('textract')
    response = client.get_document_analysis(JobId=jobId)
    
    pages.append(response)
    print("Resultset page recieved: {}".format(len(pages)))
    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']

    while(nextToken):
        time.sleep(5)

        response = client.get_document_analysis(JobId=jobId, NextToken=nextToken)

        pages.append(response)
        print("Resultset page recieved: {}".format(len(pages)))
        nextToken = None
        if('NextToken' in response):
            nextToken = response['NextToken']

    return pages

def get_rows_columns_map(table_result, blocks_map):
    rows = {}
    for relationship in table_result['Relationships']:
        if relationship['Type'] == 'CHILD':
            for child_id in relationship['Ids']:
                cell = blocks_map[child_id]
                if cell['BlockType'] == 'CELL':
                    row_index = cell['RowIndex']
                    col_index = cell['ColumnIndex']
                    if row_index not in rows:
                        # create new row
                        rows[row_index] = {}
                        
                    # get the text value
                    rows[row_index][col_index] = get_text(cell, blocks_map)
    return rows

def get_text(result, blocks_map):
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'] == 'WORD':
                        text += word['Text'] + ' '
                    if word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] =='SELECTED':
                            text +=  'X '    
    return text

def generate_table_csv(table_result, blocks_map, table_index):
    rows = get_rows_columns_map(table_result, blocks_map)

    csv = ''
    for row_index, cols in rows.items():
        
        for col_index, text in cols.items():
            csv += '{}'.format(text.replace(',', '|')) + ","
        csv += '\n'
        
    csv += '\n\n\n'
    return csv

def run_textract(bucketname, key):
    jobId = startJob(bucketname, key)
    print("Started job with id: {}".format(jobId))
    if(isJobComplete(jobId)):
        response = getJobResults(jobId)

    pagecount = 0
    cleared = False
    s3 = boto3.resource('s3')
    for page in response:
        pagecount += 1
        blocks_map = {}
        table_blocks = []
        for block in page['Blocks']:
            blocks_map[block['Id']] = block
            if block['BlockType'] == "TABLE":
                table_blocks.append(block)

        if len(table_blocks) <= 0:
            print("<b> NO Table FOUND </b>")

        csv = ''
        for index, table in enumerate(table_blocks):
            csv += generate_table_csv(table, blocks_map, index +1)
            csv += '\n\n'

        output_file = 'output_' + str(pagecount) + '_' + str(datetime.timestamp(datetime.now())) + '.csv'
    
        # replace content
        with open('/tmp/' + output_file, "wt") as fout:
            fout.write(csv)
        
        try:        
            # Upload output csv file to output s3 directory
            out_key = 'output/' + output_file
            s3.Bucket(bucketname).upload_file(Filename='/tmp/' + output_file, Key=out_key, ExtraArgs={'ContentType': 'text/csv'})
            
            # Dump the local file once it is successfully uploaded to s3
            #if os.path.exists('/tmp/' + output_file):
                #os.remove(output_file)
        except Exception as e:
            print('Failed to upload file {}'.format(output_file))
            print(e)
            raise e
        
        # Check if an output file exists in the output directory
        # If it does, move it to the archive directory
        if not(cleared):
            bucket = s3.Bucket(bucketname)
            objs = list(bucket.objects.filter(Prefix='output/output'))

            if len(objs) > 0:
                # loop through objects and move them to the archive folder
                for obj in objs:
                    key = obj.key
                    if key != out_key:
                        # Copy existing file to archive folder
                        s3.Object(bucketname, 'output/archive/' + key[7:]).copy_from(CopySource=bucketname + '/' + key)
                        
                        # Delete old file from active directory
                        obj.delete()

            # set cleared variable so we don't run through this logic again
            cleared = True
        

    