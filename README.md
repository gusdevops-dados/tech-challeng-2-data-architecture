# Pipeline Hibrido para Analise da Alfabetizacao no Brasil
**Tech Challenge - Fase 2 | Pos Tech FIAP**

## Contexto do Problema

A alfabetizacao infantil e um pilar fundamental para o desenvolvimento do Brasil. O **Compromisso Nacional Crianca Alfabetizada** estabelece a meta de que 100% das criancas estejam alfabetizadas ao final do 2o ano do ensino fundamental ate 2030. O **Indicador Crianca Alfabetizada** (ponto de corte: 743 pontos no SAEB) mede o percentual de estudantes que atingem esse nivel de proficiencia.

Para subsidiar politicas publicas baseadas em evidencias, e necessario integrar multiplas fontes de dados educacionais em uma pipeline escalavel e confiavel.

## Arquitetura da Solucao

Pipeline hibrida (Batch + Streaming) na **AWS**, seguindo a **Arquitetura Medalhao**:

```
[Base dos Dados]  +  [Eventos Simulados]
       |                      |
  AWS Glue (Batch)     Kinesis (Streaming)
       |                      |
       └──────────┬───────────┘
                  v
           BRONZE LAYER (S3/Parquet)
                  |
           AWS Glue (Silver Job)
                  v
           SILVER LAYER (S3/Parquet)
                  |
           AWS Glue (Gold Job)
                  v
           GOLD LAYER (S3/Parquet)
                  |
       ┌──────────┴──────────┐
    Athena/QuickSight    SageMaker ML
```

Ver diagrama completo em [docs/architecture/architecture_overview.md](docs/architecture/architecture_overview.md).

## Fontes de Dados

| Entidade                      | Origem        | Tipo de Ingestao |
|-------------------------------|---------------|-----------------|
| Meta Alfabetizacao Brasil     | Base dos Dados | Batch           |
| Meta Alfabetizacao por UF     | Base dos Dados | Batch           |
| Meta Alfabetizacao por Municipio | Base dos Dados | Batch        |
| Municipios                    | Base dos Dados | Batch           |
| UFs                           | Base dos Dados | Batch           |
| Atualizacoes de indicadores   | Simulado       | Streaming       |

## Estrutura do Repositorio

```
tech-challenge-fase2/
├── data/samples/              # Amostras para testes locais
├── infrastructure/terraform/  # Infraestrutura como codigo (AWS)
├── pipeline/
│   ├── ingestion/
│   │   ├── batch/             # Glue Jobs de ingestao batch
│   │   └── streaming/         # Kinesis producer + Lambda consumer
│   ├── bronze/                # Landing zone scripts
│   ├── silver/                # Transformacao e integracao
│   └── gold/                  # Camada analitica
├── quality/validations/       # Checks de qualidade de dados
├── monitoring/alerts/         # Configuracao de alarmes CloudWatch
├── docs/architecture/         # Diagrama e descricao da arquitetura
└── notebooks/exploration/     # Analise exploratoria inicial
```

## Tecnologias Utilizadas

| Tecnologia        | Uso                                  |
|-------------------|--------------------------------------|
| AWS Glue (PySpark)| Processamento batch e transformacoes |
| Amazon Kinesis    | Ingestao de eventos em streaming     |
| Amazon S3         | Data lake (Bronze/Silver/Gold)       |
| AWS Lambda        | Consumer do Kinesis                  |
| Amazon Athena     | Consultas SQL sobre camada Gold      |
| Amazon CloudWatch | Monitoramento e alertas              |
| Amazon SNS        | Notificacoes de alertas              |
| Terraform         | Infraestrutura como codigo           |
| Apache Parquet    | Formato colunar para armazenamento   |

## Decisoes Arquiteturais

### Batch vs Streaming
- **Batch**: dados historicos de metas e resultados educacionais (atualizados anualmente)
- **Streaming**: simulacao de eventos de atualizacao de indicadores em tempo quase real

### Data Lake vs Data Warehouse
- Adotado **Data Lake (S3 + Athena)** pela flexibilidade de esquema e custo vs Redshift/Snowflake

### Custo vs Performance
- Parquet com compressao Snappy reduz custo de armazenamento em ~75%
- Athena pay-per-query elimina custo de warehouse ocioso
- Kinesis com 1 shard (escala horizontal sob demanda)

## FinOps

- **Parquet + particionamento**: reduz dados escaneados no Athena
- **S3 Lifecycle**: Bronze -> S3 Glacier apos 90 dias
- **Glue Serverless**: cobra apenas por DPU-hora durante execucao
- **Athena**: cobra apenas pelos dados escaneados

## Monitoramento

Alarmes CloudWatch configurados para:
- Falhas nos Glue Jobs
- Latencia do Kinesis (IteratorAge > 60s)
- Erros na Lambda consumer
- Falhas nas validacoes de qualidade

## Aplicacao em IA (Camada Gold)

A camada Gold disponibiliza datasets prontos para:
- **Modelos preditivos**: prever percentual de alfabetizacao por municipio com base em variaveis socioeconomicas
- **Clustering**: identificar grupos de municipios com vulnerabilidade educacional similar
- **Analise de desigualdade**: series temporais de evolucao por regiao
- **Apoio a politicas publicas**: ranking de municipios prioritarios para intervencao

## Como Executar Localmente (Teste)

```bash
# Instalar dependencias
pip install pandas matplotlib seaborn

# Analise exploratoria
jupyter notebook notebooks/exploration/01_exploratory_analysis.ipynb

# Infraestrutura (requer AWS CLI configurado)
cd infrastructure/terraform
terraform init
terraform plan -var="alert_email=seu@email.com"
terraform apply
```
