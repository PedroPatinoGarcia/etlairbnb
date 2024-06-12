import boto3
from lambda_function import lambda_handler
from moto import mock_aws
import os


def upload_data_to_s3(data, bucket_name, key):
    s3_client = boto3.client('s3')

    try:
        s3_client.put_object(Body=data, Bucket=bucket_name, Key=key)
        print(f"Successfully uploaded file '{key}' to bucket '{bucket_name}'.")
    except Exception as e:
        print(f"Failed to upload file '{key}' to bucket '{bucket_name}': {e}")


def main():
    bucket_names = ["bucket1", "bucket2", "bucket3"]

    base_path = 'C:/Users/ppatinog/OneDrive - NTT DATA EMEAL/Escritorio/Proyecto Final/'
    files = ['raw.py', 'staging.py', 'business.py']

    with mock_aws():
        print("Creating buckets:")
        for bucket_name in bucket_names:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"- {bucket_name}")

        print("\nUploading files:")
        for bucket_name in bucket_names:
            for file_name in files:
                file_path = os.path.join(base_path, file_name)


                if os.path.isfile(file_path):
                    with open(file_path, 'r') as f:
                        data = f.read()
                        upload_data_to_s3(data, bucket_name, file_name)
                else:
                    print(f"Warning: File {file_path} does not exist. Skipping upload.")


if __name__ == '__main__':
    main()