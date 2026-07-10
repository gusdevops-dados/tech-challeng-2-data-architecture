# Pipeline Híbrido para Análise da Alfabetização no Brasil
**Tech Challenge - Fase 2 | Pós Tech FIAP**

## Contexto do Problema

A alfabetização infantil é um dos pilares fundamentais para o desenvolvimento educacional, social e econômico do Brasil. O **Compromisso Nacional Criança Alfabetizada** mobiliza União, estados, Distrito Federal e municípios com o objetivo de garantir que todas as crianças brasileiras estejam alfabetizadas até o final do 2º ano do ensino fundamental.

Em 2023, o INEP realizou a Pesquisa Alfabetiza Brasil e definiu o ponto de corte de **743 pontos na escala de proficiência do Saeb** como o nível a partir do qual uma criança pode ser considerada alfabetizada. A partir desse parâmetro nasceu o **Indicador Criança Alfabetizada**: o percentual de estudantes que atingem esse patamar. A meta nacional é que, até 2030, todas as crianças estejam alfabetizadas ao final do 2º ano.

Entender os fatores que influenciam a alfabetização exige integrar fontes heterogêneas — metas nacionais/estaduais/municipais, microdados de alunos e indicadores de desempenho — em uma pipeline confiável. Esse é o problema que este projeto resolve: uma equipe de engenharia de dados constrói essa pipeline para uma organização pública de análise educacional.

## Status do Projeto

Este README descreve o estado **real e testado** do pipeline, não uma arquitetura alvo. Seções marcadas como planejadas ainda não foram implementadas.

| Camada / Componente | Status |
|---|---|
| Bronze (ingestão batch) | ✅ Implementado e testado |
| Silver (limpeza, padronização, join) | ✅ Implementado e testado |
| Qualidade de dados (duplicidade, nulos, chaves, consistência) | ✅ Implementado e testado |
| Gold (camada analítica) | ✅ Implementado e testado |
| Ingestão streaming (simulação) | ✅ Implementado e testado (stream provisionado sob demanda, não fica ativo continuamente) |
| Monitoramento (CloudWatch) | 🔲 Planejado (opcional no enunciado) |
| Infraestrutura como código (Terraform) | 🔲 Esboço criado, ainda não aplicado (buckets foram criados via script Python) |

## Arquitetura da Solução (implementação atual)

A pipeline segue a **Arquitetura Medalhão** (Bronze/Silver/Gold), rodando em **AWS (S3)**, com processamento em **Python/pandas** — não há cluster Spark/Glue em execução hoje. Essa escolha é deliberada, não uma limitação: ver [Decisões Arquiteturais](#decisões-arquiteturais-trade-offs).

```
[Base dos Dados (BigQuery)]
            |
            v
   CSV local (data/raw/inep/)
            |
            | pipeline/bronze/bronze_landing.py
            | (pandas + boto3, adiciona _ingestion_date / _source_file)
            v
   BRONZE LAYER  s3://tc-fase2-alfabetizacao-bronze/<tabela>/ingestion_date=YYYY-MM-DD/data.parquet
            |
            | pipeline/silver/silver_transform.py
            | (pandas + s3fs: decodifica rede, normaliza chaves,
            |  trata nulos, unpivot de metas, valida consistência, faz join)
            v
   SILVER LAYER  s3://tc-fase2-alfabetizacao-silver/<tabela>/ingestion_date=YYYY-MM-DD/data.parquet
            |    (resultado_municipio_x_meta, resultado_uf_x_meta, alunos, meta_brasil)
            |
            | quality/validations/data_quality_checks.py
            | (roda contra o Silver: duplicidade, nulos, chaves, consistência)
            |
            | pipeline/gold/gold_analytics.py
            | (pandas: gap meta x resultado, evolução temporal por pivot, indicador por município)
            v
   GOLD LAYER  s3://tc-fase2-alfabetizacao-gold/<tabela>/ingestion_date=YYYY-MM-DD/data.parquet
            |    (indicador_municipio, comparacao_meta_municipio, comparacao_meta_uf,
            |     evolucao_temporal_municipio, evolucao_temporal_uf)

[Streaming — simulado, testado sob demanda]
   kinesis_producer.py → Kinesis stream (1 shard) → kinesis_consumer.py (polling via get_records)
        → s3://tc-fase2-alfabetizacao-bronze/streaming/indicadores/year=/month=/day=/*.json
   (o stream é criado, usado no teste e deletado logo em seguida — não fica ativo continuamente)
```

Uma visão de arquitetura de **longo prazo** (com Glue, Athena, SageMaker, CloudWatch totalmente implantados) está documentada separadamente em [docs/architecture/architecture_overview.md](docs/architecture/architecture_overview.md) — é o alvo de evolução do projeto, não o estado atual.

Decisões de implementação das camadas Bronze, Silver e Gold (motivações, achados reais nos dados, armadilhas encontradas e por que cada tratamento foi feito ou descartado): [docs/pipeline_bronze_silver.md](docs/pipeline_bronze_silver.md).

## Fluxo de Dados

1. As 6 tabelas fonte (`alunos`, `municipio`, `uf`, `meta_alfabetizacao_brasil`, `meta_alfabetizacao_uf`, `meta_alfabetizacao_municipio`) são obtidas da plataforma **Base dos Dados** via BigQuery e salvas localmente como CSV em `data/raw/inep/`.
2. **Bronze**: `bronze_landing.py` lê os CSVs, adiciona colunas de auditoria (`_ingestion_date`, `_source_file`) e grava em Parquet no S3, particionado por `ingestion_date`.
3. **Silver**: `silver_transform.py` lê a última partição de cada tabela Bronze, aplica um conjunto de regras de transformação orientadas por coluna (não por nome de tabela — cada regra só roda se a tabela tiver a coluna/tipo relevante), junta resultado real com meta por município e por UF, e grava 4 tabelas tratadas no S3.
4. **Qualidade**: `data_quality_checks.py` roda checks genéricos (duplicidade, valores ausentes, integridade de chave, consistência entre tabelas) contra as tabelas Silver, com severidade `critico` (interrompe o pipeline) ou `informativo` (só reporta).
5. **Gold**: `gold_analytics.py` lê `resultado_municipio_x_meta` e `resultado_uf_x_meta` da Silver e grava 5 tabelas prontas para consumo: `indicador_municipio` (foto do ano mais recente), `comparacao_meta_municipio`/`comparacao_meta_uf` (gap e flag `atingiu_meta`) e `evolucao_temporal_municipio`/`evolucao_temporal_uf` (taxa por ano lado a lado + variação do período).

## Fontes de Dados

| Entidade | Origem | Tipo de Ingestão |
|---|---|---|
| Dados de alunos (microdados) | Base dos Dados (BigQuery) | Batch |
| Município (resultado agregado) | Base dos Dados (BigQuery) | Batch |
| UF (resultado agregado) | Base dos Dados (BigQuery) | Batch |
| Meta Alfabetização Brasil | Base dos Dados (BigQuery) | Batch |
| Meta Alfabetização por UF | Base dos Dados (BigQuery) | Batch |
| Meta Alfabetização por Município | Base dos Dados (BigQuery) | Batch |
| Atualização de indicadores | Simulado | Streaming |

## Estrutura do Repositório

```
tech_challeng_2/
├── data/
│   ├── raw/inep/                     # CSVs brutos baixados via BigQuery (basedosdados)
│   └── credentials/                  # Credencial GCP (não versionada)
├── infrastructure/
│   ├── Scripts/create_s3_bucket.py   # Criação dos buckets Bronze/Silver/Gold (boto3)
│   └── terraform/                    # IaC — esboço de arquitetura alvo, ainda não aplicado
├── pipeline/
│   ├── bronze/bronze_landing.py      # Ingestão batch: CSV local -> S3 Parquet
│   ├── silver/silver_transform.py    # Limpeza, padronização, join, gravação no S3
│   ├── gold/gold_analytics.py        # Camada analítica: indicador, comparação meta x resultado, evolução temporal
│   └── ingestion/streaming/          # Kinesis producer + consumer (polling) — testado ponta a ponta
├── quality/validations/data_quality_checks.py  # Checks de qualidade de dados
├── monitoring/alerts/                # Esqueleto de alarmes CloudWatch — planejado
├── docs/
│   ├── architecture/architecture_overview.md   # Arquitetura alvo de longo prazo
│   └── pipeline_bronze_silver.md               # Decisões reais de implementação
└── notebooks/exploration/            # Análise exploratória inicial
```

## Tecnologias Utilizadas

| Tecnologia | Uso | Justificativa |
|---|---|---|
| Python + pandas | Transformação e limpeza dos dados | Volume atual (maior tabela ~3,8M linhas) cabe confortavelmente em memória — dispensa cluster distribuído (Glue/Spark) nesta fase |
| boto3 | Leitura/escrita de metadados e objetos no S3 | Cliente AWS direto, sem overhead de orquestração gerenciada |
| s3fs | Leitura/escrita de Parquet direto em `s3://` via pandas | Evita gerenciar downloads/uploads manuais de arquivos |
| Apache Parquet | Formato de armazenamento em todas as camadas | Colunar, comprime melhor que CSV, leitura seletiva de colunas |
| Amazon S3 | Data lake (Bronze/Silver/Gold) | Custo baixo por armazenamento, particionamento nativo por prefixo |
| Google BigQuery (via Base dos Dados) | Fonte dos dados brutos | Extração pontual dos datasets públicos do INEP já estruturados |
| Amazon Kinesis | Simulação de ingestão streaming | Managed, baixa latência — testado sob demanda (criado, usado, deletado), não fica ativo continuamente |

## Decisões Arquiteturais (trade-offs)

### Batch vs Streaming
O batch (Bronze/Silver/Gold) roda com dados persistentes, particionados por `ingestion_date`. O streaming é permitido pelo enunciado como **simulação** — testamos ponta a ponta (`kinesis_producer.py` publica eventos, `kinesis_consumer.py` lê via polling `get_records` e grava em `s3://tc-fase2-alfabetizacao-bronze/streaming/`), mas o stream Kinesis só é criado sob demanda para o teste e é deletado logo em seguida, em vez de ficar ativo continuamente — evita pagar por um shard rodando 24/7 sem um caso de uso real de dados em tempo real neste projeto ainda.

### Data Lake vs Data Warehouse
Optamos por **Data Lake (S3 + Parquet)** em vez de um Data Warehouse gerenciado (Redshift/BigQuery). Motivo: schema evolui entre as camadas (Bronze bruto → Silver tratado), e pandas lê Parquet diretamente do S3 sem custo de warehouse ocioso — só paga o que está armazenado.

### Custo vs Performance
Escolhemos processar com pandas em scripts diretos em vez de AWS Glue (PySpark gerenciado). Isso evita custo por DPU-hora e a complexidade operacional de um cluster distribuído — adequado ao volume atual. Se o volume crescer para dezenas de milhões de linhas ou mais, migrar essas mesmas transformações para Glue/Spark seria o próximo passo natural (a lógica já está isolada em funções puras `dataframe -> dataframe`, o que facilita essa migração futura).

## Qualidade de Dados

`quality/validations/data_quality_checks.py` implementa 4 checks genéricos e reutilizáveis, aplicáveis a qualquer tabela/camada:
- **Duplicidade** — nenhuma chave composta deveria se repetir (ex.: `id_aluno + ano`).
- **Valores ausentes** — colunas que nunca deveriam ser nulas (diferente de nulos documentados como esperados, como `proficiencia` quando o aluno não fez a prova — ver [docs/pipeline_bronze_silver.md](docs/pipeline_bronze_silver.md)).
- **Chave de relacionamento** — toda chave "filha" deveria existir na tabela "pai" (ex.: todo `id_municipio` em `alunos` existe em `resultado_municipio_x_meta`).
- **Consistência entre tabelas** — o mesmo dado publicado em duas fontes diferentes deveria bater.

Cada check tem uma **severidade**: `critico` interrompe o pipeline (`ValueError`) se falhar; `informativo` só reporta (usado para casos já validados como esperados, ex.: municípios sem nenhum aluno avaliado num dado ano não aparecem na tabela agregada — não é erro).

## Monitoramento e FinOps

**Monitoramento (implementado hoje):** os scripts imprimem logs de execução, e os checks críticos de qualidade falham explicitamente (exceção) quando uma regra é violada — isso já funciona como um alerta mínimo. Alarmes CloudWatch automatizados (`monitoring/alerts/`) estão planejados, mas não implantados (item opcional do enunciado).

**FinOps (implementado hoje):**
- Parquet em todas as camadas (~75% menor que CSV equivalente).
- Particionamento por `ingestion_date` — permite reprocessar sem sobrescrever e, futuramente, ler só a partição necessária.
- Nenhum recurso computacional roda continuamente: os scripts processam sob demanda e terminam; o único custo recorrente hoje é armazenamento no S3 (bronze + silver), sem cluster, warehouse ou stream ficando ociosos.
- Streaming (Kinesis): stream provisionado sob demanda só durante o teste/execução e deletado logo depois — evita pagar por um shard rodando 24/7 sem uso real.

## Aplicação em IA (Camada Gold)

Os datasets da Gold já estão prontos para:
- **Modelos preditivos**: `indicador_municipio` como base para prever a taxa de alfabetização a partir de variáveis socioeconômicas (a integrar via enriquecimento externo — ver seção de fontes opcionais do enunciado).
- **Análise de desigualdade educacional**: `comparacao_meta_municipio`/`comparacao_meta_uf` já trazem o `gap` (distância entre resultado e meta) pronto, sem precisar recalcular.
- **Apoio a políticas públicas**: ordenar `comparacao_meta_municipio` pelo maior `gap` negativo dá um ranking de municípios prioritários para intervenção.
- **Séries temporais**: `evolucao_temporal_municipio`/`evolucao_temporal_uf` trazem a variação entre os dois anos disponíveis (2023→2024) como ponto de partida para modelos de tendência quando mais anos forem incorporados.

## Como Executar Localmente

Este projeto usa `/opt/anaconda3/bin/python3` (ambiente com boto3/pandas/pyarrow/s3fs instalados) e requer credenciais AWS configuradas localmente (`~/.aws/credentials`, região `us-east-1`).

```bash
# 1. Criar os buckets S3 (Bronze/Silver/Gold), se ainda não existirem
/opt/anaconda3/bin/python3 infrastructure/Scripts/create_s3_bucket.py

# 2. Rodar a ingestão Bronze (lê CSVs locais em data/raw/inep/, grava no S3)
/opt/anaconda3/bin/python3 pipeline/bronze/bronze_landing.py

# 3. Rodar a transformação Silver (lê do Bronze, trata, junta, grava no S3)
/opt/anaconda3/bin/python3 pipeline/silver/silver_transform.py

# 4. Rodar os checks de qualidade de dados contra o Silver
/opt/anaconda3/bin/python3 quality/validations/data_quality_checks.py

# 5. Rodar a camada Gold (lê do Silver, gera os datasets analíticos, grava no S3)
/opt/anaconda3/bin/python3 pipeline/gold/gold_analytics.py

# 6. Testar a ingestão streaming (simulação) — requer criar o stream Kinesis antes
python3 -c "import boto3; boto3.client('kinesis', region_name='us-east-1').create_stream(StreamName='alfabetizacao-indicadores-stream', ShardCount=1)"
/opt/anaconda3/bin/python3 pipeline/ingestion/streaming/kinesis_producer.py
/opt/anaconda3/bin/python3 pipeline/ingestion/streaming/kinesis_consumer.py
# depois do teste, derrube o stream para não gerar custo ocioso:
python3 -c "import boto3; boto3.client('kinesis', region_name='us-east-1').delete_stream(StreamName='alfabetizacao-indicadores-stream')"
```
