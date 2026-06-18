import boto3
from botocore.config import Config

s3 = boto3.client('s3', endpoint_url='http://100.81.222.59:9000',
    aws_access_key_id='rustfsadmin', aws_secret_access_key='rustfsadmin',
    config=Config(signature_version='s3v4'), region_name='us-east-1')

import os; os.makedirs('downloaded-frames', exist_ok=True)
resp = s3.list_objects_v2(Bucket='camera-frames', Prefix='frames/', MaxKeys=10)
for obj in resp.get('Contents', []):
    fname = obj['Key'].split('/')[-1]
    s3.download_file('camera-frames', obj['Key'], f'downloaded-frames/{fname}')
    print(f'Downloaded {fname}')
