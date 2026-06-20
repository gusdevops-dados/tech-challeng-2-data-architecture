"""
Validacoes de Qualidade de Dados - Silver Layer
Checks: duplicidade, nulos em colunas criticas, integridade referencial, consistencia.
"""
import sys
import boto3
import json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from datetime import datetime

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET", "SNS_TOPIC_ARN"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_BUCKET = args["S3_BUCKET"]
SNS_TOPIC_ARN = args["SNS_TOPIC_ARN"]
sns = boto3.client("sns")

silver_path = f"s3://{S3_BUCKET}/silver/indicador_municipio/"
df = spark.read.parquet(silver_path)

results = []
failed = []


def check(name, passed, details=""):
    status = "PASS" if passed else "FAIL"
    results.append({"check": name, "status": status, "details": details})
    if not passed:
        failed.append(name)
    print(f"[{status}] {name}: {details}")


total = df.count()

# 1. Sem duplicatas
dupes = df.groupBy("id_municipio", "ano").count().filter(F.col("count") > 1).count()
check("sem_duplicatas", dupes == 0, f"{dupes} duplicatas encontradas")

# 2. Nulos em colunas criticas
for col_name in ["id_municipio", "ano", "percentual_alfabetizados"]:
    nulls = df.filter(F.col(col_name).isNull()).count()
    check(f"nulos_{col_name}", nulls == 0, f"{nulls} nulos em {col_name}")

# 3. Percentual dentro do range valido [0, 100]
out_of_range = df.filter((F.col("percentual_alfabetizados") < 0) | (F.col("percentual_alfabetizados") > 100)).count()
check("percentual_range_valido", out_of_range == 0, f"{out_of_range} registros fora do range [0,100]")

# 4. Anos validos (2019-2030)
anos_invalidos = df.filter((F.col("ano") < 2019) | (F.col("ano") > 2030)).count()
check("anos_validos", anos_invalidos == 0, f"{anos_invalidos} anos invalidos")

# 5. Chave id_municipio com 7 digitos
id_invalidos = df.filter(F.length(F.col("id_municipio").cast("string")) != 7).count()
check("formato_id_municipio", id_invalidos == 0, f"{id_invalidos} IDs com formato invalido")

print(f"\nResultado: {len(results) - len(failed)}/{len(results)} checks passaram")

if failed:
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject="[ALERTA] Falha na qualidade de dados - Silver Layer",
        Message=json.dumps({"timestamp": datetime.utcnow().isoformat(), "failed_checks": failed, "results": results}, indent=2),
    )

report_key = f"quality/reports/{datetime.utcnow().strftime('%Y-%m-%d')}/silver_indicador_municipio.json"
boto3.client("s3").put_object(
    Bucket=S3_BUCKET,
    Key=report_key,
    Body=json.dumps({"timestamp": datetime.utcnow().isoformat(), "total_records": total, "results": results}),
)

if failed:
    raise Exception(f"Qualidade de dados reprovada: {failed}")

job.commit()
