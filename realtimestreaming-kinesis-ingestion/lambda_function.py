import os
import re
from itertools import islice
import boto3
from datetime import datetime
import time
import json
import base64
import uuid
import cfnresponse
import csv
from datetime import datetime, timedelta
import urllib.request

def chunkit(l, n):
  for i in range(0, len(l), n):
    yield l[i:i + n]           

def lambda_handler(event, context):
    print(event)
    # Let AWS Cloudformation know its request succeeded
    if 'RequestType' in event:
      bucket = event['ResourceProperties']['BUCKET_NAME']
      streamName = event['ResourceProperties']['KINESIS_STREAM']
      key = event['ResourceProperties']['KEY']
      region = event['ResourceProperties']['REGION']
      responseData = {}
      cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData, "CustomResourcePhysicalID")
    else:
      bucket = event['BUCKET_NAME']
      streamName = event['KINESIS_STREAM']
      key = event['KEY']
      region = event['REGION']

    current_time = datetime.now()
    '{:%H:%M:%S}'.format( current_time )
    print(current_time)
    updated_time = datetime.now() + timedelta(minutes=14)
    print(updated_time)
    while(datetime.now() < updated_time):    
      s3 = boto3.client('s3',region)
      kinesis = boto3.client('kinesis')
      response = s3.get_object(Bucket=bucket, Key=key)
      list_obj = response['Body'].read().decode('utf-8').splitlines()
      total_records = len(list_obj)
      record_chunk_size = 50000
      chunks_whole = total_records/record_chunk_size
      chunks_floor = int(total_records/record_chunk_size)
      total_chunks = [1+chunks_floor if chunks_floor<chunks_whole else chunks_floor][0]
      print(total_chunks)
      
      record_list = [ (i*record_chunk_size, (i+1)*record_chunk_size-1)for i in range(total_chunks)]
      print(record_list)
      a,b = record_list[-1]
      b = total_records
      record_list[-1] = (a,b)
      print(record_list)
      
      header = list_obj[0]
      #print(record_list)
      for i, chunk in enumerate(record_list):
            print(chunk)
            new_list_obj = [list_obj[j] for j in range(chunk[0],chunk[1])]
            
            new_list_obj_header = [header]+new_list_obj
            print(new_list_obj_header)
            if 'csv' in key:
                  linekeypair = csv.DictReader(new_list_obj_header)
            else:
                  linekeypair = new_list_obj_header
            
            push_to_kinesis = []
            #print(linekeypair)
            for row in linekeypair:
                  if 'csv' in key:
                        datajson = json.dumps(row)
                  else:
                        datajson = row
  
                  uuidsam = uuid.uuid4()
                  partitionkey = str(uuidsam)
                  push_to_kinesis.append({"PartitionKey": partitionkey , "Data": datajson})
  
            print(push_to_kinesis)
            records = list(chunkit(push_to_kinesis,500))
            for chunk in records:
                  print("chunk")
                  print(chunk[0:10])
                  kinesis.put_records(StreamName=streamName, Records=chunk)

    return {
        'statusCode': 200,
        'body': json.dumps('Job Export Started')
    }