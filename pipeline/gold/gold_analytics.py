"""
Gold Layer - Camada Analitica
Gera datasets prontos para dashboards, analises e ML:
  1. indicador_por_municipio  - percentual alfabetizados vs meta por municipio/ano
  2. evolucao_temporal_uf     - evolucao anual por UF
  3. gap_meta_resultado        - distancia entre meta e resultado atual
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_BUCKET = args["S3_BUCKET"]
silver_base = f"s3://{S3_BUCKET}/silver"
gold_base = f"s3://{S3_BUCKET}/gold"

indicador = spark.read.parquet(f"{silver_base}/indicador_municipio/")
metas_brasil = spark.read.parquet(f"{silver_base}/meta_alfabetizacao_brasil/")

# 1. Indicador por municipio com status vs meta
gold_municipio = (
    indicador
    .join(metas_brasil.select("ano", F.col("meta").alias("meta_nacional")), on="ano", how="left")
    .withColumn("gap_meta", F.col("percentual_alfabetizados") - F.col("meta_nacional"))
    .withColumn("atingiu_meta", F.col("percentual_alfabetizados") >= F.col("meta_nacional"))
    .withColumn("_gold_timestamp", F.current_timestamp())
)
gold_municipio.write.mode("overwrite").partitionBy("ano").parquet(f"{gold_base}/indicador_por_municipio/")

# 2. Evolucao temporal por UF
window_uf = Window.partitionBy("id_uf").orderBy("ano")
gold_uf = (
    indicador
    .groupBy("id_uf", "nome_uf", "ano")
    .agg(F.avg("percentual_alfabetizados").alias("media_alfabetizados"))
    .withColumn("variacao_yoy", F.col("media_alfabetizados") - F.lag("media_alfabetizados", 1).over(window_uf))
    .withColumn("_gold_timestamp", F.current_timestamp())
)
gold_uf.write.mode("overwrite").partitionBy("ano").parquet(f"{gold_base}/evolucao_temporal_uf/")

# 3. Ranking municipios por gap em relacao a meta (para policies)
gold_gap = (
    gold_municipio
    .filter(F.col("ano") == gold_municipio.agg(F.max("ano")).collect()[0][0])
    .orderBy(F.col("gap_meta").asc())
    .withColumn("ranking_vulnerabilidade", F.rank().over(Window.orderBy(F.col("percentual_alfabetizados").asc())))
)
gold_gap.write.mode("overwrite").parquet(f"{gold_base}/ranking_vulnerabilidade_municipio/")

print("Gold layer concluida.")
job.commit()
