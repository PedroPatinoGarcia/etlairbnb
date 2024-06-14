import boto3
import json
import unittest
from lambda_function import HandlerLambda
from unittest.mock import Mock
from moto import mock_aws

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
        response = HandlerLambda(mock_event, mock_context)
        obj = s3_client.get_object(Bucket=bucket_name, Key='data.txt')
        file_content = obj['Body'].read().decode('utf-8')
        self.assertEqual(file_content, mock_event['body'])
        self.assertEqual(response['statusCode'], 200)
        self.assertEqual(json.loads(response['body'])['message'], 'File saved successfully.')

if __name__ == '__main__':
    unittest.main()