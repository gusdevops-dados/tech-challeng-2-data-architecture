import boto3
import pandas as pd
from datetime import datetime, timezone

silver_bucket = "tc-fase2-alfabetizacao-silver"
gold_bucket = "tc-fase2-alfabetizacao-gold"

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


def comparar_meta_resultado(dataframe, chave_local):
    comparavel = dataframe[dataframe["valor_meta"].notna()].copy()
    comparavel["gap"] = comparavel["taxa_alfabetizacao"] - comparavel["valor_meta"]
    comparavel["atingiu_meta"] = comparavel["gap"] >= 0

    colunas = chave_local + ["ano", "rede_padranizado", "taxa_alfabetizacao", "valor_meta", "gap", "atingiu_meta"]
    return comparavel[colunas]


def construir_evolucao_temporal(dataframe, chave_local):
    pivot = dataframe.pivot_table(
        index=chave_local + ["rede_padranizado"],
        columns="ano",
        values="taxa_alfabetizacao",
    ).reset_index()
    pivot.columns.name = None

    anos = sorted(c for c in pivot.columns if c not in chave_local + ["rede_padranizado"])
    pivot = pivot.rename(columns={ano: f"taxa_alfabetizacao_{ano}" for ano in anos})

    if len(anos) >= 2:
        mais_recente, mais_antigo = max(anos), min(anos)
        pivot["variacao_periodo"] = (
            pivot[f"taxa_alfabetizacao_{mais_recente}"] - pivot[f"taxa_alfabetizacao_{mais_antigo}"]
        )

    return pivot

def construir_indicador_municipio(dataframe):
    ano_mais_recente = dataframe["ano"].max()
    dataframe_filtrado = dataframe[dataframe["ano"] == ano_mais_recente]
    dataframe_filtrado = dataframe_filtrado[["id_municipio", "rede_padranizado", "taxa_alfabetizacao", "media_portugues"]]
    return dataframe_filtrado


def gravar_gold(dataframe, nome_tabela, data_execucao):
    caminho = f"s3://{gold_bucket}/{nome_tabela}/ingestion_date={data_execucao}/data.parquet"
    dataframe.to_parquet(caminho, index=False)
    print(f"Gravado: {caminho}")


if __name__ == "__main__":
    resultado_municipio = ler_silver("resultado_municipio_x_meta")
    resultado_uf = ler_silver("resultado_uf_x_meta")

    comparacao_meta_municipio = comparar_meta_resultado(resultado_municipio, chave_local=["id_municipio"])
    comparacao_meta_uf = comparar_meta_resultado(resultado_uf, chave_local=["sigla_uf"])

    evolucao_temporal_municipio = construir_evolucao_temporal(resultado_municipio, chave_local=["id_municipio"])
    evolucao_temporal_uf = construir_evolucao_temporal(resultado_uf, chave_local=["sigla_uf"])

    indicador_municipio = construir_indicador_municipio(resultado_municipio)

    print("comparacao_meta_municipio:", comparacao_meta_municipio.shape)
    print("comparacao_meta_uf:", comparacao_meta_uf.shape)
    print("evolucao_temporal_municipio:", evolucao_temporal_municipio.shape)
    print("evolucao_temporal_uf:", evolucao_temporal_uf.shape)
    print("Indicador_municipio:", indicador_municipio.shape)

    data_execucao = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    gravar_gold(comparacao_meta_municipio, "comparacao_meta_municipio", data_execucao)
    gravar_gold(comparacao_meta_uf, "comparacao_meta_uf", data_execucao)
    gravar_gold(evolucao_temporal_municipio, "evolucao_temporal_municipio", data_execucao)
    gravar_gold(evolucao_temporal_uf, "evolucao_temporal_uf", data_execucao)
    gravar_gold(indicador_municipio, "indicador_municipio", data_execucao)
