# Bronze, Silver e Gold — Decisões de Implementação

Este documento explica **o que foi construído e por quê** nas três camadas do pipeline (`pipeline/bronze/bronze_landing.py`, `pipeline/silver/silver_transform.py` e `pipeline/gold/gold_analytics.py`). O objetivo não é descrever "o que o código faz" (isso está no próprio código) — é registrar o raciocínio por trás de cada decisão, os achados nos dados reais que motivaram cada tratamento, e os casos em que decidimos **não** tratar algo, e por quê.

## Contexto

Fonte dos dados: 6 tabelas do INEP (Avaliação Nacional da Alfabetização), baixadas via BigQuery (`basedosdados.br_inep_avaliacao_alfabetizacao`) e salvas localmente em `data/raw/inep/`:
- `alunos` (nível aluno, ~3.8M linhas)
- `municipio` e `uf` (resultado agregado por localidade)
- `meta_alfabetizacao_brasil`, `meta_alfabetizacao_uf`, `meta_alfabetizacao_municipio` (metas oficiais de alfabetização)

## Bronze — `pipeline/bronze/bronze_landing.py`

Lê os 6 CSVs locais, adiciona colunas de auditoria (`_ingestion_date`, `_source_file`), converte para Parquet e sobe para `s3://tc-fase2-alfabetizacao-bronze/<tabela>/ingestion_date=YYYY-MM-DD/data.parquet`.

- **Por que Parquet em vez de CSV:** formato colunar, comprime melhor e é mais rápido de ler na Silver (leitura seletiva de colunas).
- **Por que particionar por `ingestion_date`:** permite reprocessar sem sobrescrever execuções anteriores. A Silver detecta automaticamente a última partição disponível (`get_ultima_particao`), então rodar a Bronze de novo não exige nenhuma mudança na Silver.

## Silver — `pipeline/silver/silver_transform.py`

### Padrão de transformação: "regras" sem hardcode por nome de tabela

Em vez de `if nome_tabela == "alunos": ...`, cada transformação é uma função `(dataframe) -> dataframe`, pareada com uma condição que verifica se **a tabela tem a coluna/tipo necessário** (`regras`, aplicada por `tratar_tabelas()`). O nome da tabela nunca aparece dentro da lógica de decisão — só a forma do dataframe.

**Por quê:** se uma 7ª tabela aparecer amanhã com uma coluna `rede` numérica, ela é tratada automaticamente, sem precisar tocar no código. Também evita o erro clássico de esquecer de aplicar uma regra numa tabela nova.

### 1. `decodificar_rede`

**Problema:** a coluna `rede` vem como código numérico (`1`, `2`, `3`...) nas tabelas `alunos`/`municipio`/`uf`, mas já como texto (`"Municipal"`, `"Pública"`) nas tabelas de meta — inconsistente entre tabelas.

**Como o dicionário foi validado:** a suposição inicial (só os códigos 1 a 4) estava incompleta. Consultamos a tabela oficial `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario` via BigQuery e descobrimos que existem também os códigos `0` (Total) e `5`/`6` (agregados de rede pública). Sem essa consulta, `.map()` teria devolvido `NaN` silencioso pra qualquer linha com código 0, 5 ou 6.

| código | rótulo |
|---|---|
| 0 | Total (Federal, Estadual, Municipal e Privada) |
| 1 | Federal |
| 2 | Estadual |
| 3 | Municipal |
| 4 | Privada |
| 5 | Pública (Estadual e Municipal) |
| 6 | Pública (Federal, Estadual e Municipal) |

**Cuidado técnico:** o tipo das chaves do dicionário precisa bater com o dtype da coluna (`int`, não `"1"` como string), senão `.map()` também devolve tudo `NaN` sem erro nenhum.

**Resultado:** a coluna original `rede` é preservada, e uma nova coluna `rede_padranizado` guarda o texto decodificado — rastreabilidade do valor original.

### 2 e 3. `proficiencia` e `proporcao_aluno_nivel_*` — decisão consciente de **não tratar**

Os dois foram investigados a fundo antes de decidir não mexer:

- **`proficiencia` (tabela `alunos`):** a suposição inicial era "nulo quando `presenca=0`". Isso é quase certo, mas incompleto: existem 1.185 linhas com `presenca=1` e `proficiencia` nula mesmo assim. O preditor exato, confirmado por crosstab, é `preenchimento_caderno == 0` (100% de precisão, sem exceções).
- **`proporcao_aluno_nivel_*` (tabelas `municipio`/`uf`):** pareciam "100% nulas" à primeira vista, mas na verdade são ~48% nulas no total — e 100% concentradas em `ano=2023`, 0% em `ano=2024`. É mudança de metodologia entre os anos da pesquisa, não uma coluna morta.

**Por que decidimos não tratar:** em ambos os casos o nulo já é semanticamente correto (não é erro de coleta). Preencher com `fillna` inventaria um dado que não existe e distorceria qualquer média calculada depois. Um `assert` de validação também foi cogitado (documentar a regra e falhar alto se ela quebrar em ingestões futuras), mas decidimos que o ganho era baixo pro escopo deste trabalho — ficou registrado aqui como decisão consciente, não como esquecimento.

### 4. `normalizar_id_municipio`

**Achado que mudou o foco do tratamento:** a motivação original era "zero à esquerda perdido" — mas isso nunca acontece nesse dataset, porque todo código de município brasileiro começa com o código de UF (11 a 53), então nunca tem menos de 7 dígitos. O problema real é **consistência de tipo entre tabelas** na hora do join: testamos e confirmamos que merge `int` x `float` funciona sem erro (pandas converte sozinho), mas merge `int`/`float` x `string` **quebra com erro explícito** (`ValueError`).

**Decisão:** converter `id_municipio` para string com `zfill(7)` nas 3 tabelas que têm essa coluna (`alunos`, `municipio`, `meta_municipio`), sempre passando por `int` antes de virar string — isso evita que um valor `float` (ex.: `3550308.0`) vire `"3550308.0"` por engano.

### 5. `unpivot_metas`

**Problema:** as metas vêm em formato wide — uma coluna por ano-alvo (`meta_alfabetizacao_2024` até `_2030`). Isso dificulta comparar com o resultado real, que é uma linha por ano.

**Como:** `value_vars` (colunas de meta) e `id_vars` (todo o resto) são calculados dinamicamente a partir do prefixo `meta_alfabetizacao_`, não hardcoded — funciona nas 3 tabelas de meta mesmo elas tendo `id_vars` diferentes (`meta_brasil` não tem localidade, `meta_uf` usa `sigla_uf`, `meta_municipio` usa `id_municipio` e tem uma coluna a mais, `nivel_alfabetizacao`). O ano-alvo é extraído do nome da coluna original via regex (`str.extract(r"(\d{4})")`).

**Achado usado depois no join:** os valores de meta são **idênticos** entre as linhas de diferentes anos-base (vintage) para o mesmo local — ou seja, o alvo pra 2030 informado na linha de 2023 é igual ao informado na linha de 2024. Isso significa que, sem deduplicar antes de juntar com outra tabela, o join dobraria as linhas.

### Validações e achados extras (mapeados às 5 categorias pedidas no trabalho)

| Categoria do trabalho | O que foi feito |
|---|---|
| Limpeza de dados | Confirmado: nenhuma linha 100% duplicada em `alunos`. Achado importante: `id_aluno` sozinho **não é uma chave estável entre anos** — o mesmo número aparece em municípios diferentes em anos diferentes (ex.: id `43074596` em 2023 no município `4309209` e em 2024 no `4316808`). A chave real e única é `id_aluno + ano`. Documentado para que ninguém use `id_aluno` sozinho como chave de dedup ou join. |
| Tratamento de valores ausentes | `proficiencia`, `proporcao_aluno_nivel_*` e `valor_meta` (pós-unpivot) — todos com nulo validado como esperado, decisão de não mexer (ver seções acima e "Join" abaixo). |
| Padronização de nomes e tipos | `rede` decodificada (`decodificar_rede`), `id_municipio` normalizado pra string de 7 dígitos, `alfabetizado` convertido de `0/1` pra `bool` (`padronizar_alfabetizado`), `sigla_uf` normalizada com `.upper().strip()` (`normalizar_sigla_uf`). |
| Validação de consistência | `validar_cobertura_uf`: a tabela `uf` (resultado real) só tem **25** siglas de UF — faltam **DF e RR**, que existem em `meta_uf` mas com valores nulos/incompletos. Isso é logado automaticamente toda vez que o script roda. `validar_id_municipio`: confere que todo código tem 7 dígitos e começa com um prefixo de UF oficial (lista fechada dos 27 códigos válidos). |
| Normalização de chaves | `id_municipio` (ver item 4). `sigla_uf` normalizada como chave de join. Chave real de `alunos` documentada como composta (`id_aluno + ano`, não `id_aluno` sozinho). |

### 6. Join — `juntar_resultado_com_meta`

**Decisão de design:** o join junta o resultado real (`municipio`, `uf`) com a meta correspondente (`meta_municipio`, `meta_uf`) — **não** uma megatabela única incluindo `alunos`. Motivo: `alunos` está numa granularidade completamente diferente (por estudante); juntar com dado agregado de município causaria fan-out (o mesmo dado de cidade repetido em cada uma das milhares de linhas de aluno daquele município) sem ganho real. `alunos` e `meta_brasil` são gravados como tabelas separadas na Silver.

Três armadilhas reais foram encontradas e corrigidas durante a implementação — todas descobertas rodando o merge contra os dados de verdade, não hipoteticamente:

1. **Coluna de rede ausente do lado meta.** As tabelas de meta nunca passam por `decodificar_rede` (o `rede` delas já vem como texto), então elas não têm `rede_padranizado` — só `rede`. O join usa `rede_padranizado` do lado resultado e `rede` do lado meta.
2. **Rótulos de rede pública não batem entre tabelas.** `uf` decodifica o código 5 como `"Pública (Estadual e Municipal)"`, mas `meta_uf` e `meta_brasil` usam só `"Pública"`. Comparação exata de string daria **zero matches** silenciosamente. Resolvido com `agrupar_rede_publica()`, que reduz qualquer rótulo começando com `"Pública"` a um valor comum só para fins de chave de join (sem alterar a coluna original).
3. **Duplicação de metas entre anos-base.** Como descrito no unpivot, os valores de meta se repetem entre as linhas de "vintage" (`ano`) — sem deduplicar antes do merge, cada resultado casaria com múltiplas linhas de meta idênticas, inflando a contagem de linhas. Resolvido com `.drop_duplicates()` na tabela de metas antes do merge.

**Validação do resultado:** os números batem com achados anteriores — `resultado_municipio_x_meta` tem 5.232/23.995 linhas com meta preenchida (só casa quando `rede="Municipal"` e `ano=2024`, já que a meta só existe para 2024-2030 e a tabela só tem 2023/2024). `resultado_uf_x_meta` tem 24/145 (25 UFs têm resultado — sem DF/RR — menos o Acre, que tem `meta_2024` nula na fonte).

### 7. Gravação no bucket Silver

Quatro tabelas finais gravadas em Parquet em `s3://tc-fase2-alfabetizacao-silver/<tabela>/ingestion_date=YYYY-MM-DD/data.parquet`:
- `resultado_municipio_x_meta`
- `resultado_uf_x_meta`
- `alunos`
- `meta_brasil`

Escrita direto via `to_parquet("s3://...")` usando `s3fs` (a mesma biblioteca já usada pra leitura), sem precisar do padrão `io.BytesIO` + `upload_fileobj` usado na Bronze.

## Gold — `pipeline/gold/gold_analytics.py`

Lê as 2 tabelas de resultado da Silver (`resultado_municipio_x_meta`, `resultado_uf_x_meta`) e gera 5 tabelas analíticas, cobrindo os 3 pedidos do enunciado (indicador por município, comparação meta x resultado, evolução temporal). Segue o mesmo padrão de leitura por partição da Silver (`get_ultima_particao` + `ler_silver`).

### `comparar_meta_resultado` → `comparacao_meta_municipio`, `comparacao_meta_uf`

Função genérica (recebe `dataframe` + `chave_local`, funciona tanto pra município quanto pra UF) que calcula `gap = taxa_alfabetizacao - valor_meta` e a flag `atingiu_meta = gap >= 0`.

**Decisão:** a função filtra primeiro por `valor_meta.notna()`, descartando linhas sem meta definida, em vez de manter todas as linhas com `gap` nulo. Motivo: essa tabela é especificamente uma "comparação" — uma linha sem meta não tem o que comparar, e mantê-la só adicionaria ruído num dataset pensado para consumo direto em dashboard. Isso é diferente da decisão tomada na Silver de manter nulos "informativos" — ali o objetivo era preservar o dado bruto tratado; aqui o objetivo é entregar um dataset pronto para uma pergunta específica.

**Resultado:** 5.232 linhas em `comparacao_meta_municipio` e 24 em `comparacao_meta_uf` — os mesmos números que já apareciam no join da Silver (ver seção acima), confirmando que o filtro não introduziu nem perdeu nada de inesperado.

### `construir_evolucao_temporal` → `evolucao_temporal_municipio`, `evolucao_temporal_uf`

Usa `pivot_table` para transformar `ano` de linha em coluna (`taxa_alfabetizacao_2023`, `taxa_alfabetizacao_2024`, etc.) e calcula `variacao_periodo` (ano mais recente menos o mais antigo).

**Pré-condição que torna o pivot seguro:** `pivot_table` agrega silenciosamente (média, por padrão) se o índice (`chave_local + rede_padranizado`) não for único por `ano` — isso poderia mascarar um bug sem erro nenhum. O módulo de qualidade (`quality/validations/data_quality_checks.py`) já confirma via `check_duplicidade` que `ano + id_municipio + rede_padranizado` (e o equivalente em UF) é uma chave única na Silver, então o pivot aqui não agrega nada de fato — só reorganiza.

**Decisão de generalização:** o ano mais recente/mais antigo são calculados dinamicamente (`max(anos)`/`min(anos)`), não hardcoded como `2024`/`2023` — quando uma nova partição com 2025 chegar, a função não precisa mudar.

### `construir_indicador_municipio` → `indicador_municipio`

A mais simples das 3: filtra `resultado_municipio_x_meta` para o ano mais recente (`dataframe["ano"].max()`, também dinâmico) e seleciona `id_municipio`, `rede_padranizado`, `taxa_alfabetizacao`, `media_portugues`. Sem cálculo — é a "foto" mais atual do indicador por município.

**Resultado:** 12.448 linhas — exatamente metade das 23.995 linhas de `resultado_municipio_x_meta` (que tem 2 anos), confirmando que o filtro por ano mais recente pegou só uma fatia, como esperado.

### Gravação no bucket Gold

5 tabelas gravadas em `s3://tc-fase2-alfabetizacao-gold/<tabela>/ingestion_date=YYYY-MM-DD/data.parquet`: `comparacao_meta_municipio`, `comparacao_meta_uf`, `evolucao_temporal_municipio`, `evolucao_temporal_uf`, `indicador_municipio`.
