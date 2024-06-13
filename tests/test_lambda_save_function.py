import unittest
from unittest.mock import patch, Mock
from moto import mock_s3
import boto3
import json
from lambda_save_function import upload_data_to_s3

class TestLambdaSaveFunction(unittest.TestCase):

    @mock_s3
    def test_upload_data_to_s3(self):
        s3_client = boto3.client('s3', region_name='us-east-1')
        bucket_name = 'test-bucket'
        s3_client.create_bucket(Bucket=bucket_name)

        mock_data = b'This is mock data.'
        mock_key = 'test-file.txt'

        upload_data_to_s3(mock_data, bucket_name, mock_key)

        response = s3_client.get_object(Bucket=bucket_name, Key=mock_key)
        body = response['Body'].read().decode('utf-8')

        self.assertEqual(body, 'This is mock data.')

if __name__ == '__main__':
    unittest.main()
