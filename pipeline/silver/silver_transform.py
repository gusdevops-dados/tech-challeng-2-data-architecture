"""
Silver Layer - Transformacao e Limpeza
- Limpeza de nulos e duplicatas
- Padronizacao de nomes e tipos
- Normalizacao de chaves (id_municipio, id_uf)
- Integracao entre bases (metas + municipios + UFs)
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET", "INGESTION_DATE"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_BUCKET = args["S3_BUCKET"]
INGESTION_DATE = args["INGESTION_DATE"]

bronze_path = f"s3://{S3_BUCKET}/bronze"
silver_path = f"s3://{S3_BUCKET}/silver"


def read_bronze(table_name):
    return spark.read.parquet(f"{bronze_path}/{table_name}/ingestion_date={INGESTION_DATE}/")


def clean_dataframe(df, key_cols):
    df = df.dropDuplicates(key_cols)
    for col_name, col_type in df.dtypes:
        if col_type == "string":
            df = df.withColumn(col_name, F.trim(F.col(col_name)))
            df = df.withColumn(col_name, F.when(F.col(col_name) == "", None).otherwise(F.col(col_name)))
    return df


metas_municipio = read_bronze("meta_alfabetizacao_municipio")
municipios = read_bronze("municipios")
ufs = read_bronze("ufs")

metas_municipio = clean_dataframe(metas_municipio, ["id_municipio", "ano"])
municipios = clean_dataframe(municipios, ["id_municipio"])
ufs = clean_dataframe(ufs, ["sigla"])

metas_municipio = metas_municipio.withColumn("percentual_alfabetizados", F.col("percentual_alfabetizados").cast(DoubleType()))
metas_municipio = metas_municipio.withColumn("ano", F.col("ano").cast(IntegerType()))

silver_df = (
    metas_municipio
    .join(municipios.select("id_municipio", "nome", "id_uf"), on="id_municipio", how="left")
    .join(ufs.select(F.col("sigla").alias("id_uf"), F.col("nome").alias("nome_uf")), on="id_uf", how="left")
    .withColumn("_silver_timestamp", F.current_timestamp())
)

silver_df.write.mode("overwrite").partitionBy("ano", "id_uf").parquet(f"{silver_path}/indicador_municipio/")
print(f"Silver concluido: {silver_df.count()} registros")

job.commit()
