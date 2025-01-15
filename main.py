## Import
import os
from typing import List
import boto3
from dotenv import load_dotenv


# Load the env variables
load_dotenv() 

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
BUCKET_NAME = os.getenv('BUCKET_NAME')


# Setting the S3 Client
try:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
except Exception as ex:
    print('Error to configure the s3 Cliente: ', ex)

## Read the files
def read_files(folder):
    files = []
    try:
        for file_name in os.listdir(folder):
            path = os.path.join(folder, file_name)
            if os.path.isfile(path):
                files.append(path)
        return files
    
    except Exception as ex:
        print(f'Error to list files from folder: {folder}: ', ex)


## Put the files into S3
def upload_files_s3(files):
    for file in files:
        name_file = os.path.basename(file)
        try:
            s3_client.upload_file(file, BUCKET_NAME, name_file)
            print(f'{name_file} was uploaded to S3')
        except Exception as ex:
            print(f'Error to upload {name_file} to S3: ', ex)

## Delete the files in S3
def delete_files_from_local(files):
    for file in files:
        try:
            os.remove(file)
            print(f'{file} was deleted from local')
        except Exception as ex:
            print(f'Error to delete file {file}: ', ex)

## Pipeline
def run_pipeline_bkp(folder):
    files = read_files(folder)
    if files:
        upload_files_s3(files)
        delete_files_from_local(files)
    else:
        print('There is no files to backup')


if __name__ == '__main__':
    LOCAL_FOLDER = 'arquivos_test'
    try:
        run_pipeline_bkp(LOCAL_FOLDER)
    except Exception as ex:
        print('Error to run the bkp: ', ex)



