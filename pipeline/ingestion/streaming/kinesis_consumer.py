"""
Kinesis Consumer (Lambda) - Consome eventos do stream e persiste na Bronze Layer.
Trigger: AWS Lambda via EventSourceMapping no Kinesis stream.
"""
import json
import base64
import boto3
from datetime import datetime

S3_BUCKET = "tc-fase2-alfabetizacao"
s3 = boto3.client("s3")


def lambda_handler(event, context):
    records = event.get("Records", [])
    print(f"Processando {len(records)} registros do Kinesis...")

    for record in records:
        payload = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
        data = json.loads(payload)

        ts = datetime.utcnow()
        key = (
            f"bronze/streaming/indicadores/"
            f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/"
            f"{ts.strftime('%H%M%S%f')}.json"
        )

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(data),
            ContentType="application/json",
        )
        print(f"Gravado: s3://{S3_BUCKET}/{key}")

    return {"statusCode": 200, "body": f"{len(records)} registros processados"}
