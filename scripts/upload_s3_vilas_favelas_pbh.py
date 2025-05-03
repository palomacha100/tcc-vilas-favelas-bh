import boto3
import pandas as pd

s3 = boto3.client('s3')

bucket_name = 'tcc-eng-software-vilas-favelas-bh'
file_name = './data/raw/vilas-favelas-bh.csv'
object_key = 'bronze/vilas-favelas-bh.csv'

s3.upload_file(file_name, bucket_name, object_key)

print(f"Arquivo {file_name} carregado com sucesso para {bucket_name}/{object_key}.")

