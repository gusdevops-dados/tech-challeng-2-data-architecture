import boto3
from numpy.random import f
import pandas as pd
import pyarrow 
import os
from datetime import datetime, timezone
import io

bronze_bucket = "tc-fase2-alfabetizacao-bronze"
silver_bucket = "tc-fase2-alfabetizacao-silver"

client = boto3.client('s3', region_name='us-east-1')

tabelas = [
    "br_inep_avaliacao_alfabetizacao_alunos",
    "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil",
    "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio",
    "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf",
    "br_inep_avaliacao_alfabetizacao_uf",
    "br_inep_avaliacao_alfabetizacao_municipio"
]

def get_ultima_particao(bucket, prefixo):
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefixo)
    chaves = [obj['Key'] for obj in response.get('Contents', [])]

    particoes = set()
    for chave in chaves:
        partes = chave.split('/')
        for parte in partes:
            if parte.startswith('ingestion_date='):
                particoes.add(parte)

    if not particoes:
        raise ValueError(f"Nenhuma partição encontrada em s3://{bucket}/{prefixo}")

    return sorted(particoes)[-1]

def ler_tabelas(tabela, particao): 
    s3_path = f"s3://{bronze_bucket}/{tabela}/{particao}/data.parquet"
    df = pd.read_parquet(path=s3_path)
    return df


ultimas_particoes = {}
data_frames = {}
for tabela in tabelas:
    prefixo = f"{tabela}/"
    ultimas_particoes[tabela] = get_ultima_particao(bronze_bucket, prefixo)
    print(f"{tabela}: {ultimas_particoes[tabela]}")

    data_frames[tabela] = ler_tabelas(tabela=tabela, particao=ultimas_particoes[tabela])
    print(data_frames[tabela])


def decodificar_rede(dataframe):
    rede_map = {
        0: "Total (Federal, Estadual, Municipal e Privada)",
        1: "Federal",
        2: "Estadual",
        3: "Municipal",
        4: "Privada",
        5: "Pública (Estadual e Municipal)",
        6: "Pública (Federal, Estadual e Municipal)",
    }
    dataframe['rede_padranizado'] = dataframe['rede'].map(rede_map)
    return dataframe

def normalizar_id_municipio(dataframe):
    dataframe["id_municipio"] = dataframe["id_municipio"].astype(int).astype(str).str.zfill(7)
    return dataframe

def unpivot_metas(dataframe):
    value_vars = [c for c in dataframe.columns if c.startswith("meta_alfabetizacao_")]
    id_vars = [c for c in dataframe.columns if c not in value_vars]

    dataframe_long = dataframe.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name="meta_alfabetizacao_coluna",
        value_name="valor_meta",
    )
    dataframe_long["ano_meta"] = dataframe_long["meta_alfabetizacao_coluna"].str.extract(r"(\d{4})").astype(int)
    dataframe_long = dataframe_long.drop(columns="meta_alfabetizacao_coluna")

    return dataframe_long

def padronizar_alfabetizado(dataframe):
    dataframe["alfabetizado"] = dataframe["alfabetizado"].astype(bool)
    return dataframe

def normalizar_sigla_uf(dataframe):
    dataframe["sigla_uf"] = dataframe["sigla_uf"].str.upper().str.strip()
    return dataframe

CODIGOS_UF_VALIDOS = {
    "11", "12", "13", "14", "15", "16", "17",
    "21", "22", "23", "24", "25", "26", "27", "28", "29",
    "31", "32", "33", "35",
    "41", "42", "43",
    "50", "51", "52", "53",
}

def validar_id_municipio(dataframe):
    formato_valido = dataframe["id_municipio"].str.len() == 7
    prefixo_valido = dataframe["id_municipio"].str[:2].isin(CODIGOS_UF_VALIDOS)
    invalidos = dataframe[~(formato_valido & prefixo_valido)]

    if not invalidos.empty:
        print(f"[validação] {len(invalidos)} id_municipio fora do formato esperado (7 dígitos, prefixo de UF válido).")
    else:
        print("[validação] Todos os id_municipio no formato válido.")

    return dataframe


regras = [
    (lambda df: "rede" in df.columns and df["rede"].dtype != object, decodificar_rede),
    (lambda df: "id_municipio" in df.columns, normalizar_id_municipio),
    (lambda df: "id_municipio" in df.columns, validar_id_municipio),
    (lambda df: "sigla_uf" in df.columns, normalizar_sigla_uf),
    (lambda df: "alfabetizado" in df.columns, padronizar_alfabetizado),
    (lambda df: any(c.startswith("meta_alfabetizacao_") for c in df.columns), unpivot_metas),
]



def tratar_tabelas():
    for nome_tabela, df in data_frames.items():
        for condicao, funcao in regras:
            if condicao(df):
                df = funcao(df)
        data_frames[nome_tabela] = df
    return


def validar_cobertura_uf(dataframe_uf, dataframe_meta_uf):
    ufs_resultado = set(dataframe_uf["sigla_uf"].unique())
    ufs_meta = set(dataframe_meta_uf["sigla_uf"].unique())

    faltando_no_resultado = ufs_meta - ufs_resultado
    faltando_na_meta = ufs_resultado - ufs_meta

    if faltando_no_resultado:
        print(f"[validação] UFs com meta definida mas sem resultado em 'uf': {sorted(faltando_no_resultado)}")
    if faltando_na_meta:
        print(f"[validação] UFs com resultado mas sem meta definida: {sorted(faltando_na_meta)}")
    if not faltando_no_resultado and not faltando_na_meta:
        print("[validação] Cobertura de UF consistente entre 'uf' e 'meta_uf'.")

    return faltando_no_resultado, faltando_na_meta


def agrupar_rede_publica(serie_rede):
    return serie_rede.where(~serie_rede.str.startswith("Pública"), "Pública")


def juntar_resultado_com_meta(dataframe_resultado, dataframe_meta, chave_local):
    resultado = dataframe_resultado.copy()
    meta = dataframe_meta.copy()

    resultado["grupo_rede"] = agrupar_rede_publica(resultado["rede_padranizado"])
    meta["grupo_rede"] = agrupar_rede_publica(meta["rede"])

    metas_unicas = meta[chave_local + ["grupo_rede", "ano_meta", "valor_meta"]].drop_duplicates()

    resultado_join = resultado.merge(
        metas_unicas,
        left_on=chave_local + ["grupo_rede", "ano"],
        right_on=chave_local + ["grupo_rede", "ano_meta"],
        how="left",
    )
    resultado_join = resultado_join.drop(columns=["grupo_rede", "ano_meta"])
    return resultado_join


tratar_tabelas()

validar_cobertura_uf(
    data_frames["br_inep_avaliacao_alfabetizacao_uf"],
    data_frames["br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf"],
)

resultado_municipio_x_meta = juntar_resultado_com_meta(
    data_frames["br_inep_avaliacao_alfabetizacao_municipio"],
    data_frames["br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio"],
    chave_local=["id_municipio"],
)

resultado_uf_x_meta = juntar_resultado_com_meta(
    data_frames["br_inep_avaliacao_alfabetizacao_uf"],
    data_frames["br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf"],
    chave_local=["sigla_uf"],
)

print("resultado_municipio_x_meta: linhas com valor_meta preenchido:",
      resultado_municipio_x_meta["valor_meta"].notna().sum(), "/", len(resultado_municipio_x_meta))
print("resultado_uf_x_meta: linhas com valor_meta preenchido:",
      resultado_uf_x_meta["valor_meta"].notna().sum(), "/", len(resultado_uf_x_meta))


def gravar_silver(dataframe, nome_tabela, data_execucao):
    caminho = f"s3://{silver_bucket}/{nome_tabela}/ingestion_date={data_execucao}/data.parquet"
    dataframe.to_parquet(caminho, index=False)
    print(f"Gravado: {caminho}")


data_execucao = datetime.now(timezone.utc).strftime("%Y-%m-%d")

gravar_silver(resultado_municipio_x_meta, "resultado_municipio_x_meta", data_execucao)
gravar_silver(resultado_uf_x_meta, "resultado_uf_x_meta", data_execucao)
gravar_silver(data_frames["br_inep_avaliacao_alfabetizacao_alunos"], "alunos", data_execucao)
gravar_silver(data_frames["br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil"], "meta_brasil", data_execucao)