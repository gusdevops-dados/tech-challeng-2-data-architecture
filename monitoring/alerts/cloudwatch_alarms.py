"""
CloudWatch Alarms - Configuracao via boto3
Cria alarmes para monitoramento operacional da pipeline:
  - Falhas de Jobs Glue
  - Latencia do Kinesis
  - Volume de dados processados
  - Erros na Lambda consumer
"""
import boto3

cw = boto3.client("cloudwatch", region_name="us-east-1")
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:ACCOUNT_ID:alfabetizacao-alerts"

ALARMS = [
    {
        "AlarmName": "glue-ingest-batch-failures",
        "AlarmDescription": "Falhas no job Glue de ingestao batch",
        "MetricName": "glue.driver.aggregate.numFailedTasks",
        "Namespace": "Glue",
        "Statistic": "Sum",
        "Period": 300,
        "EvaluationPeriods": 1,
        "Threshold": 1,
        "ComparisonOperator": "GreaterThanOrEqualToThreshold",
        "Dimensions": [{"Name": "JobName", "Value": "glue-ingest-metas"}],
    },
    {
        "AlarmName": "kinesis-consumer-errors",
        "AlarmDescription": "Erros na Lambda consumer do Kinesis",
        "MetricName": "Errors",
        "Namespace": "AWS/Lambda",
        "Statistic": "Sum",
        "Period": 60,
        "EvaluationPeriods": 2,
        "Threshold": 1,
        "ComparisonOperator": "GreaterThanOrEqualToThreshold",
        "Dimensions": [{"Name": "FunctionName", "Value": "kinesis-consumer-alfabetizacao"}],
    },
    {
        "AlarmName": "kinesis-iterator-age-high",
        "AlarmDescription": "Latencia alta no processamento do stream Kinesis",
        "MetricName": "GetRecords.IteratorAgeMilliseconds",
        "Namespace": "AWS/Kinesis",
        "Statistic": "Maximum",
        "Period": 300,
        "EvaluationPeriods": 2,
        "Threshold": 60000,
        "ComparisonOperator": "GreaterThanThreshold",
        "Dimensions": [{"Name": "StreamName", "Value": "alfabetizacao-indicadores-stream"}],
    },
    {
        "AlarmName": "data-quality-check-failures",
        "AlarmDescription": "Falhas nas validacoes de qualidade de dados",
        "MetricName": "Errors",
        "Namespace": "AWS/Glue",
        "Statistic": "Sum",
        "Period": 300,
        "EvaluationPeriods": 1,
        "Threshold": 1,
        "ComparisonOperator": "GreaterThanOrEqualToThreshold",
        "Dimensions": [{"Name": "JobName", "Value": "data-quality-checks"}],
    },
]


def create_alarms():
    for alarm in ALARMS:
        cw.put_metric_alarm(
            **alarm,
            ActionsEnabled=True,
            AlarmActions=[SNS_TOPIC_ARN],
            OKActions=[SNS_TOPIC_ARN],
            TreatMissingData="notBreaching",
        )
        print(f"Alarme criado/atualizado: {alarm['AlarmName']}")


if __name__ == "__main__":
    create_alarms()
    print("Todos os alarmes configurados com sucesso.")
