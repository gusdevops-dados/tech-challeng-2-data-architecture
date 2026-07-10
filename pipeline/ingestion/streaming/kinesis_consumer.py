"""
Kinesis Consumer - Consome eventos do stream e persiste na Bronze Layer.

Implementado como script de polling direto via boto3 (get_shard_iterator + get_records),
em vez de AWS Lambda: mantém a mesma filosofia pragmática do resto do pipeline
(scripts diretos, sem infraestrutura gerenciada adicional pra manter/deployar).
"""
import json
import time
import boto3
from datetime import datetime, timezone

STREAM_NAME = "alfabetizacao-indicadores-stream"
REGION = "us-east-1"
BRONZE_BUCKET = "tc-fase2-alfabetizacao-bronze"

kinesis = boto3.client("kinesis", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def obter_iterador_inicial(stream_name):
    shard_id = kinesis.describe_stream(StreamName=stream_name)["StreamDescription"]["Shards"][0]["ShardId"]
    return kinesis.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON",
    )["ShardIterator"]


def gravar_evento_bronze(evento):
    ts = datetime.now(timezone.utc)
    key = (
        f"streaming/indicadores/"
        f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/"
        f"{ts.strftime('%H%M%S%f')}.json"
    )
    s3.put_object(Bucket=BRONZE_BUCKET, Key=key, Body=json.dumps(evento), ContentType="application/json")
    print(f"Gravado: s3://{BRONZE_BUCKET}/{key}")
    return key


def consumir(stream_name=STREAM_NAME, max_iteracoes=10, espera_segundos=2, parar_apos_vazias=3):
    iterador = obter_iterador_inicial(stream_name)
    iteracoes_vazias = 0
    total_processado = 0

    for _ in range(max_iteracoes):
        resposta = kinesis.get_records(ShardIterator=iterador, Limit=10)
        registros = resposta["Records"]

        if not registros:
            iteracoes_vazias += 1
            if iteracoes_vazias >= parar_apos_vazias:
                break
        else:
            iteracoes_vazias = 0

        for registro in registros:
            evento = json.loads(registro["Data"])
            gravar_evento_bronze(evento)
            total_processado += 1

        iterador = resposta["NextShardIterator"]
        time.sleep(espera_segundos)

    print(f"Consumo finalizado. Total de eventos processados: {total_processado}")
    return total_processado


if __name__ == "__main__":
    consumir()
