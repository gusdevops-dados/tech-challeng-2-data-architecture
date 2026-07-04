import boto3
import pandas as pd


def check_duplicidade(dataframe, colunas_chave, severidade="critico"):
    duplicadas = dataframe.duplicated(subset=colunas_chave).sum()
    return {
        "check": "duplicidade",
        "colunas": colunas_chave,
        "severidade": severidade,
        "status": "OK" if duplicadas == 0 else "FALHA",
        "linhas_afetadas": int(duplicadas),
    }


def check_valores_ausentes(dataframe, colunas_obrigatorias, severidade="critico"):
    contagem = {c: int(dataframe[c].isna().sum()) for c in colunas_obrigatorias}
    tem_nulo = any(v > 0 for v in contagem.values())
    return {
        "check": "valores_ausentes",
        "severidade": severidade,
        "status": "OK" if not tem_nulo else "FALHA",
        "detalhe": contagem,
    }


def check_chave_relacionamento(dataframe_filho, dataframe_pai, chave, severidade="informativo"):
    orfaos = set(dataframe_filho[chave].unique()) - set(dataframe_pai[chave].unique())
    return {
        "check": "chave_relacionamento",
        "chave": chave,
        "severidade": severidade,
        "status": "OK" if not orfaos else "FALHA",
        "total_orfaos": len(orfaos),
        "amostra_orfaos": sorted(orfaos)[:20],
    }


def check_consistencia_entre_tabelas(dataframe_a, dataframe_b, chave, coluna, tolerancia=0, severidade="informativo"):
    comparacao = dataframe_a[[chave, coluna]].merge(
        dataframe_b[[chave, coluna]], on=chave, suffixes=("_a", "_b")
    )
    diferenca = (comparacao[f"{coluna}_a"] - comparacao[f"{coluna}_b"]).abs()
    divergentes = comparacao[diferenca > tolerancia]
    return {
        "check": "consistencia_entre_tabelas",
        "coluna": coluna,
        "severidade": severidade,
        "status": "OK" if divergentes.empty else "FALHA",
        "linhas_comparadas": len(comparacao),
        "linhas_divergentes": len(divergentes),
    }


def gerar_relatorio(resultados):
    return pd.DataFrame(resultados)


def validar_criticos(resultados):
    falhas_criticas = [r for r in resultados if r["severidade"] == "critico" and r["status"] == "FALHA"]
    if falhas_criticas:
        raise ValueError(f"{len(falhas_criticas)} check(s) critico(s) falharam: {falhas_criticas}")


if __name__ == "__main__":
    silver_bucket = "tc-fase2-alfabetizacao-silver"
    client = boto3.client("s3", region_name="us-east-1")

    def get_ultima_particao(bucket, prefixo):
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefixo)
        chaves = [obj["Key"] for obj in response.get("Contents", [])]
        particoes = {parte for chave in chaves for parte in chave.split("/") if parte.startswith("ingestion_date=")}
        if not particoes:
            raise ValueError(f"Nenhuma partição encontrada em s3://{bucket}/{prefixo}")
        return sorted(particoes)[-1]

    def ler_silver(tabela):
        particao = get_ultima_particao(silver_bucket, f"{tabela}/")
        return pd.read_parquet(f"s3://{silver_bucket}/{tabela}/{particao}/data.parquet")

    alunos = ler_silver("alunos")
    resultado_municipio = ler_silver("resultado_municipio_x_meta")
    resultado_uf = ler_silver("resultado_uf_x_meta")

    resultados = [
        check_duplicidade(alunos, ["id_aluno", "ano"], severidade="critico"),
        check_valores_ausentes(alunos, ["ano", "id_municipio", "id_aluno", "rede_padranizado"], severidade="critico"),
        check_duplicidade(resultado_municipio, ["ano", "id_municipio", "rede_padranizado"], severidade="critico"),
        check_valores_ausentes(
            resultado_municipio, ["ano", "id_municipio", "rede_padranizado", "taxa_alfabetizacao"], severidade="critico"
        ),
        check_duplicidade(resultado_uf, ["ano", "sigla_uf", "rede_padranizado"], severidade="critico"),
        check_valores_ausentes(
            resultado_uf, ["ano", "sigla_uf", "rede_padranizado", "taxa_alfabetizacao"], severidade="critico"
        ),
        check_chave_relacionamento(alunos, resultado_municipio, "id_municipio", severidade="informativo"),
    ]

    relatorio = gerar_relatorio(resultados)
    print(relatorio.to_string())

    validar_criticos(resultados)
    print("\nTodos os checks críticos passaram.")
