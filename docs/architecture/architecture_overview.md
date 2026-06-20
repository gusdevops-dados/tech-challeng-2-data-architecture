# Arquitetura - Pipeline Hibrido de Alfabetizacao

## Visao Geral

Pipeline hibrida (Batch + Streaming) na AWS seguindo Arquitetura Medalhao (Bronze/Silver/Gold).

## Diagrama de Fluxo

```
[Base dos Dados / BigQuery]          [Eventos Simulados]
         |                                    |
         | (Batch - AWS Glue)                 | (Streaming - Kinesis)
         v                                    v
  ┌─────────────────────────────────────────────────────┐
  │                  BRONZE LAYER (S3)                  │
  │  s3://bucket/bronze/<tabela>/ingestion_date=YYYY-MM │
  │  Formato: Parquet | Historico completo preservado   │
  └─────────────────────────┬───────────────────────────┘
                            │ (AWS Glue - silver_transform.py)
                            v
  ┌─────────────────────────────────────────────────────┐
  │                  SILVER LAYER (S3)                  │
  │  Limpeza + Padronizacao + Integracao das bases      │
  │  Particoes: ano / id_uf                             │
  └─────────────────────────┬───────────────────────────┘
                            │ (AWS Glue - gold_analytics.py)
                            v
  ┌─────────────────────────────────────────────────────┐
  │                   GOLD LAYER (S3)                   │
  │  indicador_por_municipio                            │
  │  evolucao_temporal_uf                               │
  │  ranking_vulnerabilidade_municipio                  │
  └──────────┬─────────────────────────┬────────────────┘
             │                         │
             v                         v
      [AWS Athena]              [SageMaker / ML]
      [QuickSight]              [Modelos preditivos]
```

## Tecnologias

| Componente     | Tecnologia          | Justificativa                          |
|----------------|---------------------|----------------------------------------|
| Ingestao Batch | AWS Glue (PySpark)  | Serverless, integrado ao S3/Glue Catalog |
| Streaming      | Amazon Kinesis      | Managed, baixa latencia, escalavel     |
| Armazenamento  | Amazon S3 + Parquet | Custo baixo, colunar, compressao eficiente |
| Orquestracao   | AWS Glue Workflows  | Nativo, sem custo de servidor          |
| Qualidade      | PySpark + SNS       | Validacoes customizadas + alertas      |
| Monitoramento  | CloudWatch + SNS    | Alarmes, dashboards, notificacoes      |
| IaC            | Terraform           | Reproducibilidade, versionamento       |
| Consulta       | Amazon Athena       | Serverless SQL sobre S3 (pay-per-query)|

## FinOps - Decisoes de Custo

- **Parquet + Snappy**: reducao de ~75% no tamanho vs CSV
- **Particionamento**: reduz dados escaneados no Athena
- **S3 Lifecycle**: Bronze -> Glacier apos 90 dias
- **Glue Serverless**: sem custo de cluster ocioso
- **Kinesis 1 shard**: suficiente para carga simulada (escala sob demanda)
- **Athena pay-per-query**: sem custo de warehouse permanente
