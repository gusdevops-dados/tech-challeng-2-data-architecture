"""
AWS Glue Job - Ingestão Batch: Metas de Alfabetização
Fonte: Base dos Dados (basedosdados.org)
Destino: S3 Bronze Layer (Parquet)
"""
import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from datetime import datetime

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET", "BQ_PROJECT"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_BUCKET = args["S3_BUCKET"]
BQ_PROJECT = args["BQ_PROJECT"]
INGESTION_DATE = datetime.utcnow().strftime("%Y-%m-%d")

TABLES = {
    "meta_alfabetizacao_brasil": "basedosdados.br_inep_indicador_crianca_alfabetizada.brasil",
    "meta_alfabetizacao_uf": "basedosdados.br_inep_indicador_crianca_alfabetizada.uf",
    "meta_alfabetizacao_municipio": "basedosdados.br_inep_indicador_crianca_alfabetizada.municipio",
    "municipios": "basedosdados.br_bd_diretorios_brasil.municipio",
    "ufs": "basedosdados.br_bd_diretorios_brasil.uf",
}

for table_name, bq_table in TABLES.items():
    print(f"Ingerindo tabela: {bq_table}")
    df = (
        spark.read.format("bigquery")
        .option("table", bq_table)
        .option("project", BQ_PROJECT)
        .load()
    )
    df = df.withColumn("_ingestion_date", F.lit(INGESTION_DATE))
    df = df.withColumn("_source_table", F.lit(bq_table))

    output_path = f"s3://{S3_BUCKET}/bronze/{table_name}/ingestion_date={INGESTION_DATE}/"
    df.write.mode("overwrite").parquet(output_path)
    print(f"Escrito em: {output_path} | Registros: {df.count()}")

job.commit()
