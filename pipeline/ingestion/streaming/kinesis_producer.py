"""
Kinesis Producer - Simula eventos de atualização de indicadores em tempo quase real.
Publica eventos de atualização de metas/resultados no stream Kinesis.
"""
import json
import boto3
import random
import time
from datetime import datetime

STREAM_NAME = "alfabetizacao-indicadores-stream"
REGION = "us-east-1"

kinesis = boto3.client("kinesis", region_name=REGION)

SAMPLE_UFS = ["SP", "RJ", "MG", "BA", "RS", "PR", "PE", "CE", "PA", "MA"]


def generate_indicador_event():
    return {
        "event_type": "indicador_atualizado",
        "timestamp": datetime.utcnow().isoformat(),
        "uf": random.choice(SAMPLE_UFS),
        "ano": 2024,
        "percentual_alfabetizados": round(random.uniform(55.0, 95.0), 2),
        "meta_nacional": 100.0,
        "fonte": "SAEB",
    }


def publish_events(n_events: int = 10, interval_seconds: float = 1.0):
    print(f"Publicando {n_events} eventos no stream '{STREAM_NAME}'...")
    for i in range(n_events):
        event = generate_indicador_event()
        kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(event),
            PartitionKey=event["uf"],
        )
        print(f"[{i+1}/{n_events}] Evento publicado: UF={event['uf']} | {event['percentual_alfabetizados']}%")
        time.sleep(interval_seconds)
    print("Publicacao concluida.")


if __name__ == "__main__":
    publish_events(n_events=20, interval_seconds=0.5)
