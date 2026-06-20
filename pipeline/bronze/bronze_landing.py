"""
Bronze Layer - Landing Zone
Consolida dados brutos (batch + streaming) no S3 sem transformações significativas.
Garante histórico completo com particionamento por data de ingestão.
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET", "TABLE_NAME", "INGESTION_DATE"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_BUCKET = args["S3_BUCKET"]
TABLE_NAME = args["TABLE_NAME"]
INGESTION_DATE = args["INGESTION_DATE"]

input_path = f"s3://{S3_BUCKET}/raw/{TABLE_NAME}/"
output_path = f"s3://{S3_BUCKET}/bronze/{TABLE_NAME}/ingestion_date={INGESTION_DATE}/"

df = spark.read.option("mergeSchema", "true").parquet(input_path)

df = (
    df.withColumn("_bronze_timestamp", F.current_timestamp())
      .withColumn("_ingestion_date", F.lit(INGESTION_DATE))
)

df.write.mode("overwrite").parquet(output_path)

print(f"Bronze landing concluido: {df.count()} registros -> {output_path}")
job.commit()
