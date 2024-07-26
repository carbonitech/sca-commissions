from dotenv import load_dotenv; load_dotenv()
from os import getenv
import boto3
from entities.commission_file import CommissionFile

AWS_ACCESS_ID = getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = getenv('S3_BUCKET_NAME')
TESTING_ENDPOINT = getenv('ENDPOINT')

s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_ID,
    aws_secret_access_key=AWS_SECRET_KEY,
    endpoint_url=TESTING_ENDPOINT
)

def get_file(obj_path: str) -> bytes:
    response: dict = s3_client.get_object(Bucket=BUCKET_NAME, Key=obj_path)
    if response.get('HTTPStatusCode') == 200:
        file_data = response.get('Body').read()
        file_content_type: str = response.get('ContentType')
        return file_content_type, file_data

def upload_file(file: CommissionFile, dest: str) -> None:
    s3_client.put_object( 
        Body=file.file_data,
        Bucket=BUCKET_NAME,
        Key=dest,
        ContentType=file.file_mime
    )