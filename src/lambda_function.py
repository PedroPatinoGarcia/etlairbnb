import json
import boto3
from botocore.exceptions import ClientError
import os

def lambda_handler():
    s3_client = boto3.client('s3')
    bucket_name = 'nmpbucketairbnb'
    file_name = 'data-2024-06-13.csv'  # Nombre del archivo que quieres subir

    try:
        # Crear el bucket si no existe
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' creado exitosamente.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print(f"El bucket '{bucket_name}' ya existe y te pertenece.")
        else:
            print(f"Error al crear el bucket '{bucket_name}': {e}")
            raise

    # Ruta completa al archivo que quieres subir
    file_path = os.path.join('./business/2024/6/13', file_name)

    try:
        with open(file_path, 'rb') as f:
            file_content = f.read()
            s3_client.put_object(Body=file_content, Bucket=bucket_name, Key=file_name)
        print(f"Archivo '{file_name}' subido correctamente al bucket '{bucket_name}'.")
    except FileNotFoundError:
        print(f"Error: Archivo {file_name} no encontrado en la ruta {file_path}.")
    except ClientError as e:
        print(f"Error al subir {file_name} al bucket {bucket_name}: {e}")
        raise

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'File saved successfully.'})
    }

if __name__ == '__main__':
    print(lambda_handler())


# class TestLambdaFunction(unittest.TestCase):

#     @mock_aws
#     def test_lambda_handler(self):
#         s3_client = boto3.client('s3', region_name='us-east-1')
#         bucket_name = 'bucket'
#         s3_client.create_bucket(Bucket=bucket_name)

#         mock_event = {
#             'body': 'This is the file content.',
#             'file_name': 'data.txt'
#         }

#         mock_context = Mock()
#         response = lambda_handler(mock_event, mock_context)
#         obj = s3_client.get_object(Bucket=bucket_name, Key='data.txt')
#         file_content = obj['Body'].read().decode('utf-8')
#         self.assertEqual(file_content, mock_event['body'])
#         self.assertEqual(response['statusCode'], 200)
#         self.assertEqual(json.loads(response['body'])['message'], 'File saved successfully.')

# if __name__ == '__main__':
#     unittest.main()