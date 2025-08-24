# etl_s3_totvs.py
import pandas as pd
import boto3
import io

# S3 Credentials

AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = 'fiap-meraki-match-totvs'
PASTA = 'dados/'

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
)

# Utils

def ler_csv(nome_arquivo: str) -> pd.DataFrame:
    """Lê um CSV do S3 com fallback de encoding e separador ';'."""
    key = f"{PASTA}{nome_arquivo}"
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    conteudo = obj["Body"].read()
    try:
        return pd.read_csv(io.BytesIO(conteudo), encoding="utf-8", sep=";", on_bad_lines="skip")
    except UnicodeDecodeError:
        print(f" Aviso: {nome_arquivo} não está em UTF-8. Tentando Latin-1...")
        return pd.read_csv(io.BytesIO(conteudo), encoding="latin1", sep=";", on_bad_lines="skip")

def salvar_local(df: pd.DataFrame, nome_saida: str):
    df.to_csv(f"{nome_saida}.csv", index=False, encoding="utf-8")
    print(f"{nome_saida}.csv salvo com sucesso.")

def uniformiza_chave_cliente(df: pd.DataFrame) -> pd.DataFrame:
    """
    Garante que exista a coluna CD_CLIENTE.
    Copia de alternativas comuns: CLIENTE, IdCliente, ID_CLIENTE, COD_CLIENTE, CODIGO_CLIENTE,
    CD_CLI, metadata_codcliente.
    """
    possiveis = [
        "CD_CLIENTE", "CLIENTE", "IdCliente", "ID_CLIENTE",
        "COD_CLIENTE", "CODIGO_CLIENTE", "CD_CLI", "metadata_codcliente"
    ]
    existentes = [c for c in possiveis if c in df.columns]
    if not existentes:
        return df
    src = existentes[0]
    if "CD_CLIENTE" not in df.columns:
        df = df.copy()
        df["CD_CLIENTE"] = df[src]
    return df

# ---------- Blocos de tratamento ----------

def tratar_nps():
    arquivos = [
        "nps_relacional.csv",
        "nps_transacional_aquisicao.csv",
        "nps_transacional_implantacao.csv",
        "nps_transacional_onboarding.csv",
        "nps_transacional_produto.csv",
        "nps_transacional_suporte.csv",
    ]
    dfs = []
    for nome in arquivos:
        try:
            df = ler_csv(nome)
            df["origem_nps"] = nome.replace(".csv", "")
            dfs.append(df)
        except Exception as e:
            print(f" Falha lendo {nome}: {e}")

    if not dfs:
        print("⚠ NPS: nenhum arquivo lido com sucesso.")
        return

    nps = pd.concat(dfs, ignore_index=True)
    nps = nps.dropna(how="all").drop_duplicates()

    # uniformiza chave de cliente
    nps = uniformiza_chave_cliente(nps)

    # normaliza coluna de NPS
    candidatos_nps = [c for c in nps.columns if c.lower() in ("nps", "resposta_nps", "nota_nps")]
    if candidatos_nps:
        nps = nps.rename(columns={candidatos_nps[0]: "NPS"})
        nps["NPS"] = pd.to_numeric(nps["NPS"], errors="coerce")

    salvar_local(nps, "nps_tratado")

def tratar_tickets():
    df = ler_csv("tickets.csv")
    print(" Colunas disponíveis em tickets.csv:", df.columns.tolist())
    df = df.dropna(how="all").drop_duplicates()

    # cria 'TempoResolucao' se houver alguma coluna parecida; senão, preenche zero
    tempo_col = None
    for col in df.columns:
        norm = (col.lower()
                  .replace("ç", "c")
                  .replace("ã", "a")
                  .replace("õ", "o")
                  .replace("é", "e")
                  .replace("ó", "o"))
        if "resolucao" in norm or ("tempo" in norm and "res" in norm):
            tempo_col = col
            break
    if tempo_col:
        df["TempoResolucao"] = pd.to_numeric(df[tempo_col], errors="coerce").fillna(0)
    else:
        print(" Coluna de tempo de resolução não encontrada. Preenchendo com 0.")
        df["TempoResolucao"] = 0

    # uniformiza chave, se existir
    df = uniformiza_chave_cliente(df)

    salvar_local(df, "tickets_tratado")

    # Agregação por organização (já que não há CD_CLIENTE nessa base)
    if "CODIGO_ORGANIZACAO" in df.columns:
        agg = (df.groupby("CODIGO_ORGANIZACAO")
                 .agg(QTD_CHAMADOS=("BK_TICKET", "count"),
                      CHAMADOS_ABERTOS=("STATUS_TICKET", lambda s: (s.astype(str).str.upper()=="ABERTO").sum()),
                      TEMPO_MEDIO_RES=("TempoResolucao","mean"))
                 .reset_index())
        salvar_local(agg, "tickets_agg_organizacao")

def tratar_vendas():
    vendas = ler_csv("mrr.csv")
    contratos = ler_csv("contratacoes_ultimos_12_meses.csv")

    print(" Colunas em mrr.csv:", vendas.columns.tolist())
    print(" Colunas em contratacoes_ultimos_12_meses.csv:", contratos.columns.tolist())

    # uniformiza chaves
    vendas = uniformiza_chave_cliente(vendas)
    contratos = uniformiza_chave_cliente(contratos)

    # numéricos
    if "MRR_12M" in vendas.columns:
        vendas["MRR_12M"] = (
            vendas["MRR_12M"].astype(str).str.replace(",", ".", regex=False)
        )
        vendas["MRR_12M"] = pd.to_numeric(vendas["MRR_12M"], errors="coerce")

    for col in ["QTD_CONTRATACOES_12M", "VLR_CONTRATACOES_12M"]:
        if col in contratos.columns:
            contratos[col] = (
                contratos[col].astype(str).str.replace(",", ".", regex=False)
            )
            contratos[col] = pd.to_numeric(contratos[col], errors="coerce")

    if "CD_CLIENTE" not in vendas.columns or "CD_CLIENTE" not in contratos.columns:
        print(" Não foi possível identificar a chave de cliente em mrr/contratos.")
        salvar_local(vendas, "mrr_sem_merge_inspecao")
        salvar_local(contratos, "contratos_sem_merge_inspecao")
        return

    df = pd.merge(vendas, contratos, how="left", on="CD_CLIENTE")
    df = df.dropna(how="all").drop_duplicates()

    salvar_local(df, "vendas_tratado")

def tratar_clientes():
    base = ler_csv("dados_clientes.csv")
    desde = ler_csv("clientes_desde.csv")
    historico = ler_csv("historico.csv")

    print(" Colunas em dados_clientes.csv:", base.columns.tolist())
    print(" Colunas em clientes_desde.csv:", desde.columns.tolist())
    print(" Colunas em historico.csv:", historico.columns.tolist())

    base = uniformiza_chave_cliente(base)
    desde = uniformiza_chave_cliente(desde)
    historico = uniformiza_chave_cliente(historico)

    if "CD_CLIENTE" not in base.columns:
        print(" Não foi possível identificar a chave de cliente em dados_clientes.csv")
        salvar_local(base, "dados_clientes_inspecao")
        return

    df = base.copy()

    if "CD_CLIENTE" in desde.columns:
        df = pd.merge(df, desde, how="left", on="CD_CLIENTE")
    else:
        print(" clientes_desde.csv sem chave unificada; salvando para inspeção.")
        salvar_local(desde, "clientes_desde_inspecao")

    if "CD_CLIENTE" in historico.columns:
        df = pd.merge(df, historico, how="left", on="CD_CLIENTE")
    else:
        print(" historico.csv sem chave unificada; salvando para inspeção.")
        salvar_local(historico, "historico_inspecao")

    df = df.dropna(how="all").drop_duplicates()
    salvar_local(df, "clientes_tratado")

def tratar_telemetria():
    dfs = []
    for i in range(1, 12):
        nome = f"telemetria_{i}.csv"
        try:
            df = ler_csv(nome)
            df["fonte"] = nome
            df = uniformiza_chave_cliente(df)
            dfs.append(df)
        except Exception as e:
            print(f"️ Telemetria: falha lendo {nome}: {e}")

    if not dfs:
        print(" Nenhum arquivo de telemetria lido.")
        return

    df_tele = pd.concat(dfs, ignore_index=True)
    df_tele = df_tele.dropna(how="all").drop_duplicates()
    salvar_local(df_tele, "telemetria_tratado")

def construir_base_analitica():
    """
    Constrói dataset consolidado por CD_CLIENTE:
    - Clientes (perfil)
    - Vendas (MRR_12M, QTD_CONTRATACOES_12M, VLR_CONTRATACOES_12M)
    - NPS (média por cliente)
    """
    try:
        clientes = pd.read_csv("clientes_tratado.csv", encoding="utf-8")
        vendas   = pd.read_csv("vendas_tratado.csv",   encoding="utf-8")
        nps      = pd.read_csv("nps_tratado.csv",      encoding="utf-8")
    except Exception as e:
        print(f" Erro lendo arquivos tratados: {e}")
        return

    # garantir chaves
    clientes = uniformiza_chave_cliente(clientes)
    vendas   = uniformiza_chave_cliente(vendas)
    nps      = uniformiza_chave_cliente(nps)

    # NPS médio por cliente (se coluna NPS existir)
    if "NPS" in nps.columns and "CD_CLIENTE" in nps.columns:
        nps_agg = (nps.groupby("CD_CLIENTE")["NPS"].mean()
                     .reset_index()
                     .rename(columns={"NPS": "NPS_MEDIO"}))
    else:
        nps_agg = pd.DataFrame(columns=["CD_CLIENTE", "NPS_MEDIO"])

    # Seleção de colunas relevantes
    cols_clientes = [c for c in clientes.columns if c in (
        "CD_CLIENTE", "DS_SEGMENTO", "DS_SUBSEGMENTO", "FAT_FAIXA", "UF", "CIDADE", "VL_TOTAL_CONTRATO", "DT_ASSINATURA_CONTRATO"
    )] or ["CD_CLIENTE"]
    clientes_sel = clientes[cols_clientes].drop_duplicates("CD_CLIENTE")

    cols_vendas = [c for c in vendas.columns if c in (
        "CD_CLIENTE", "MRR_12M", "QTD_CONTRATACOES_12M", "VLR_CONTRATACOES_12M"
    )] or ["CD_CLIENTE"]
    vendas_sel = vendas[cols_vendas].drop_duplicates("CD_CLIENTE")

    # Merge final
    base = clientes_sel.merge(vendas_sel, on="CD_CLIENTE", how="left")\
                       .merge(nps_agg,     on="CD_CLIENTE", how="left")

    salvar_local(base, "base_analitica_meraki")

# ---------- Execução ----------

if __name__ == "__main__":
    tratar_nps()
    tratar_tickets()
    tratar_vendas()
    tratar_clientes()
    tratar_telemetria()
    construir_base_analitica()
    print(" ETL finalizado.")
