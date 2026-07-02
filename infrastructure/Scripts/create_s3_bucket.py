import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3", region_name="us-east-1")

def create_s3_bucket(bucket_name):
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} criado com sucesso")
    except ClientError as e:
        codigo = e.response['Error']['Code']
        if codigo == 'BucketAlreadyOwnedByYou':
            print(f"Bucket ja existe: {bucket_name}")
        else:
            raise 


camadas = [
    "bronze",
    "silver",
    "gold"
]

for camada in camadas:
    create_s3_bucket(f"tc-fase2-alfabetizacao-{camada}")