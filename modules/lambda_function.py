import json
import boto3
from botocore.exceptions import ClientError
import unittest
from unittest.mock import Mock
from moto import mock_aws

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    bucket_name = 'bucket'

    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            pass
        else:
            raise

    file_content = event['body']
    file_name = event['file_name']
    s3_client.put_object(Body=file_content, Bucket=bucket_name, Key=file_name)

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'File saved successfully.'})
    }

class TestLambdaFunction(unittest.TestCase):

    @mock_aws
    def test_lambda_handler(self):
        s3_client = boto3.client('s3', region_name='us-east-1')
        bucket_name = 'bucket'
        s3_client.create_bucket(Bucket=bucket_name)

        mock_event = {
            'body': 'This is the file content.',
            'file_name': 'data.txt'
        }

        mock_context = Mock()
        response = lambda_handler(mock_event, mock_context)
        obj = s3_client.get_object(Bucket=bucket_name, Key='data.txt')
        file_content = obj['Body'].read().decode('utf-8')
        self.assertEqual(file_content, mock_event['body'])
        self.assertEqual(response['statusCode'], 200)
        self.assertEqual(json.loads(response['body'])['message'], 'File saved successfully.')

if __name__ == '__main__':
    unittest.main()