
import boto3
from botocore import UNSIGNED
import botocore
from botocore.config import Config
import os

bucket_name = 'pc-bulk'
s3_folder = 'CA13_Guo/'  
local_dir = '/workspace/Data-Lake/'

s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED), endpoint_url='https://opentopography.s3.sdsc.edu')

try:
    objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_folder)
except botocore.exceptions.ClientError as e:
    print("Couldn't list objects in the bucket. Here's why: ", e)
    exit()

for obj in objects.get('Contents', []):
    s3_file_key = obj['Key']
    if not s3_file_key.endswith('/'):
        file_name = os.path.basename(s3_file_key)
        local_file_path = os.path.join(local_dir, file_name)
        
        os.makedirs(local_dir, exist_ok=True)
        
        try:
            s3_client.download_file(bucket_name, s3_file_key, local_file_path)
            print(f'Downloaded {file_name} to {local_file_path}')
        except botocore.exceptions.ClientError as e:
            print(f"Couldn't download {file_name}. Here's why: ", e)


