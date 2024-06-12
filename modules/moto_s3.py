import boto3
from moto import mock_aws


def create_buckets(s3_client):
    s3_client.create_bucket(Bucket='raw-data')
    s3_client.create_bucket(Bucket='staging-data')
    s3_client.create_bucket(Bucket='business-data')


def upload_files(s3_client):
    raw_data = b"raw data"
    staging_data = b"staging data"
    business_data = b"business data"

    s3_client.put_object(Bucket='raw-data', Key='raw.py', Body=raw_data)
    s3_client.put_object(Bucket='staging-data', Key='staging.py', Body=staging_data)
    s3_client.put_object(Bucket='business-data', Key='business.py', Body=business_data)


def list_files(s3_client, bucket_name):
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    if 'Contents' in response:
        for obj in response['Contents']:
            print(f"{bucket_name}: {obj['Key']}")
    else:
        print(f"{bucket_name} is empty")


@mock_aws
def main():
    s3_client = boto3.client('s3')

    create_buckets(s3_client)

    upload_files(s3_client)

    list_files(s3_client, 'raw-data')
    list_files(s3_client, 'staging-data')
    list_files(s3_client, 'business-data')


if __name__ == "__main__":
    main()
