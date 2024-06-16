import json
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
import os

def HandlerBranchLambda():
    """
    Función para manejar la lógica de subida de archivos a un bucket S3 usando AWS Lambda.

    Returns:
        dict: Un diccionario con el estado de la operación y un mensaje en formato JSON.
    """
    s3_client = boto3.client('s3')
    bucket_name = 'nmpbucketairbnb'
    today_date = datetime.now().strftime('%Y-%m-%d')
    file_name = f'data-{today_date}.csv'

    try:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' creado exitosamente.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print(f"El bucket '{bucket_name}' ya existe y te pertenece.")
        else:
            print(f"Error al crear el bucket '{bucket_name}': {e}")
            raise

    file_path = os.path.join('./business', f'data-{today_date}', file_name)

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
    print(HandlerBranchLambda())