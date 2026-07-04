from tkinter.constants import Y
import boto3
import pandas as pd
import pyarrow 
import os
from datetime import datetime, timezone
import io

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_PATH = os.path.join(BASE_DIR, "../../data/raw/inep")

BUCKET = "tc-fase2-alfabetizacao-bronze"
CLIENT = boto3.client("s3", region_name="us-east-1")

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

def listar_arquivos(pasta: str) -> list[str]:
    return [
        os.path.join(pasta, f)
        for f in os.listdir(pasta)
        if os.path.isfile(os.path.join(pasta, f))
    ]

def processar_arquivo(arquivo: str):
    df = pd.read_csv(arquivo)
    df["_ingestion_date"] = TODAY
    df["_source_file"] = os.path.basename(arquivo) 
    return df


def upload_para_s3(df, nome_tabela):
    buffer = io.BytesIO()
    
    df.to_parquet(buffer, index=False)
    
    buffer.seek(0)

    s3_key = f"{nome_tabela}/ingestion_date={TODAY}/data.parquet"
    
    CLIENT.upload_fileobj(buffer, BUCKET, s3_key)
    print(f"Upload concluido: s3://{BUCKET}/{s3_key}")

def executar():
    for arquivo in listar_arquivos(BASE_PATH):
        nome_tabela = os.path.splitext(os.path.basename(arquivo))[0]
        df = processar_arquivo(arquivo)
        upload_para_s3(df, nome_tabela)

executar()