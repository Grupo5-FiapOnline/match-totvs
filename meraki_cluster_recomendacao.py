import os
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

def _to_numeric_br(series: pd.Series) -> pd.Series:
    # remove espaços, remove separador de milhar ".", troca vírgula por ponto
    s = (series.astype(str)
               .str.replace(r"\s+", "", regex=True)
               .str.replace(".", "", regex=False)
               .str.replace(",", ".", regex=False))
    return pd.to_numeric(s, errors="coerce")

# ==============================
# 1) CARREGAR/RECONSTRUIR BASE
# ==============================
def carregar_ou_construir_base():
    base_path = "base_analitica_meraki.csv"
    if os.path.exists(base_path):
        print(" Lendo base_analitica_meraki.csv")
        return pd.read_csv(base_path)

    print(" base_analitica_meraki.csv não encontrada. Reconstruindo a partir dos tratados...")
    # Arquivos do ETL
    clientes = pd.read_csv("clientes_tratado.csv", encoding="utf-8")
    vendas   = pd.read_csv("vendas_tratado.csv",   encoding="utf-8")
    nps      = pd.read_csv("nps_tratado.csv",      encoding="utf-8")

    # Garantir chave unificada
    def uni(df):
        poss = ["CD_CLIENTE","CLIENTE","IdCliente","ID_CLIENTE","COD_CLIENTE","CODIGO_CLIENTE","CD_CLI","metadata_codcliente"]
        for c in poss:
            if c in df.columns:
                if "CD_CLIENTE" not in df.columns:
                    df = df.copy()
                    df["CD_CLIENTE"] = df[c]
                break
        return df

    clientes = uni(clientes)
    vendas   = uni(vendas)
    nps      = uni(nps)

    # NPS médio por cliente
    if "NPS" in nps.columns and "CD_CLIENTE" in nps.columns:
        nps["NPS"] = pd.to_numeric(nps["NPS"], errors="coerce")
        nps_agg = nps.groupby("CD_CLIENTE", as_index=False)["NPS"].mean().rename(columns={"NPS":"NPS_MEDIO"})
    else:
        nps_agg = pd.DataFrame(columns=["CD_CLIENTE","NPS_MEDIO"])

    # Selecionar colunas úteis dos clientes
    cols_cli = [
        "CD_CLIENTE","DS_SEGMENTO","DS_SUBSEGMENTO","FAT_FAIXA","UF","CIDADE",
        "VL_TOTAL_CONTRATO","DT_ASSINATURA_CONTRATO"
    ]
    cols_cli = [c for c in cols_cli if c in clientes.columns]
    clientes_sel = clientes[cols_cli].drop_duplicates(subset=["CD_CLIENTE"])

    # Selecionar colunas úteis de vendas
    cols_v = ["CD_CLIENTE","MRR_12M","QTD_CONTRATACOES_12M","VLR_CONTRATACOES_12M"]
    cols_v = [c for c in cols_v if c in vendas.columns]
    vendas_sel = vendas[cols_v].drop_duplicates(subset=["CD_CLIENTE"])

    # Merge final
    base = clientes_sel.merge(vendas_sel, how="left", on="CD_CLIENTE")\
                       .merge(nps_agg,     how="left", on="CD_CLIENTE")

    # Conversões numéricas robustas
    for col in ["VL_TOTAL_CONTRATO","MRR_12M","QTD_CONTRATACOES_12M","VLR_CONTRATACOES_12M","NPS_MEDIO"]:
        if col in base.columns:
            base[col] = base[col].astype(str).str.replace(",", ".", regex=False)
            base[col] = pd.to_numeric(base[col], errors="coerce")

    base.to_csv(base_path, index=False, encoding="utf-8")
    print(" base_analitica_meraki.csv gerada.")
    return base

# =================================
# 2) FEATURE ENGINEERING & LIMPEZA
# =================================
def preparar_features(base: pd.DataFrame):
    df = base.copy()

    # Antiguidade (meses) a partir de DT_ASSINATURA_CONTRATO (se existir)
    if "DT_ASSINATURA_CONTRATO" in df.columns:
        df["DT_ASSINATURA_CONTRATO"] = pd.to_datetime(
            df["DT_ASSINATURA_CONTRATO"], errors="coerce", dayfirst=True
        )
        hoje = pd.Timestamp.today()
        df["ANTIGUIDADE_MESES"] = ((hoje - df["DT_ASSINATURA_CONTRATO"]).dt.days / 30.44).round(1)
    else:
        df["ANTIGUIDADE_MESES"] = np.nan

    # Seleciona features numéricas candidatas (use as que existirem)
    features_num = [
        "MRR_12M",
        "QTD_CONTRATACOES_12M",
        "VLR_CONTRATACOES_12M",
        "NPS_MEDIO",
        "VL_TOTAL_CONTRATO",
        "ANTIGUIDADE_MESES",
    ]
    features_num = [c for c in features_num if c in df.columns]

    # Converte todas as features numéricas de formato BR -> float
    for c in features_num:
        df[c] = _to_numeric_br(df[c]) if df[c].dtype == "O" else pd.to_numeric(df[c], errors="coerce")

    # Completa faltantes com a mediana
    for c in features_num:
        med = df[c].median()
        df[c] = df[c].fillna(med)

    # (Opcional) one-hot em 1-2 categorias de alto nível, se existirem
    cat_cols = []
    if "DS_SEGMENTO" in df.columns:
        cat_cols.append("DS_SEGMENTO")
    if "FAT_FAIXA" in df.columns:
        cat_cols.append("FAT_FAIXA")

    if cat_cols:
        dummies = pd.get_dummies(df[cat_cols], prefix=cat_cols, drop_first=True)
        X = pd.concat([df[features_num], dummies], axis=1)
    else:
        dummies = pd.DataFrame(index=df.index)  # vazio
        X = df[features_num].copy()

    # Padronização
    from sklearn.preprocessing import StandardScaler
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    feature_names = features_num + list(dummies.columns)
    return df, X, X_scaled, feature_names


# ======================================
# 3) ESCOLHA DO k (SILHOUETTE) + KMEANS
# ======================================
def treinar_kmeans(X_scaled, ks=[3,4,5,6], random_state=42):
    best_k, best_score, best_model = None, -1, None
    for k in ks:
        km = KMeans(n_clusters=k, random_state=random_state, n_init="auto")
        labels = km.fit_predict(X_scaled)
        score = silhouette_score(X_scaled, labels)
        print(f"k={k} | silhouette={score:.4f}")
        if score > best_score:
            best_k, best_score, best_model = k, score, km
    print(f" Melhor k={best_k} (silhouette={best_score:.4f})")
    return best_model

# ======================================
# 4) PERFIL DE CLUSTER + SALVAMENTOS
# ======================================
def salvar_resultados(df, X, labels, feature_names):
    """
    df: dataframe original (com CD_CLIENTE e colunas de negócio)
    X:  dataframe de features usadas no KMeans (com dummies, numéricas, etc.)
    labels: array com o cluster de cada linha
    feature_names: nomes das colunas de X (usado se X não for DataFrame)
    """
    # 1) clusters_clientes.csv (mapa cliente -> cluster)
    out_clientes = df[["CD_CLIENTE"]].copy()
    out_clientes["cluster"] = labels
    out_clientes.to_csv("clusters_clientes.csv", index=False, encoding="utf-8")
    print(" clusters_clientes.csv salvo.")

    # 2) Construir DF de features (garantir DataFrame mesmo se X for ndarray)
    if isinstance(X, pd.DataFrame):
        feats_df = X.copy()
    else:
        feats_df = pd.DataFrame(X, columns=feature_names)

    feats_df["cluster"] = labels

    # 3) Perfil numérico dos clusters (médias das features)
    perfil = feats_df.groupby("cluster").mean(numeric_only=True).reset_index()

    # 4) Contagem por segmento no df original (opcional)
    seg_count = pd.DataFrame()
    if "DS_SEGMENTO" in df.columns:
        df_seg = df.copy()
        df_seg["cluster"] = labels
        seg_count = (df_seg.groupby(["cluster", "DS_SEGMENTO"])["CD_CLIENTE"]
                          .count()
                          .reset_index()
                          .rename(columns={"CD_CLIENTE": "QTD"}))

    # 5) Salvar resumo em Excel
    with pd.ExcelWriter("cluster_summary.xlsx", engine="xlsxwriter") as xlw:
        perfil.to_excel(xlw, index=False, sheet_name="metricas_medias")
        if not seg_count.empty:
            seg_count.to_excel(xlw, index=False, sheet_name="segmento_contagem")

    print(" cluster_summary.xlsx salvo.")


# ======================================
# 5) RECOMENDAÇÃO: TOP PRODUTOS POR CLUSTER
# ======================================
def gerar_recomendacoes(labels):
    """
    Estratégia simples:
    - Usa clientes_tratado.csv para ver 'DS_PROD' (produto atual)
    - Para cada cluster, encontra os TOP produtos mais comuns
    - Para cada cliente, recomenda TOP-N do cluster que ele ainda não possui
    """
    try:
        clientes = pd.read_csv("clientes_tratado.csv", encoding="utf-8")
    except Exception as e:
        print(f" Não foi possível ler clientes_tratado.csv: {e}")
        return

    # Unificar chave
    def uni(df):
        poss = ["CD_CLIENTE","CLIENTE","IdCliente","ID_CLIENTE","COD_CLIENTE","CODIGO_CLIENTE","CD_CLI","metadata_codcliente"]
        for c in poss:
            if c in df.columns:
                if "CD_CLIENTE" not in df.columns:
                    df = df.copy()
                    df["CD_CLIENTE"] = df[c]
                break
        return df
    clientes = uni(clientes)

    # Produtos atuais do cliente
    cols_min = ["CD_CLIENTE","DS_PROD"]
    cols_min = [c for c in cols_min if c in clientes.columns]
    if not set(["CD_CLIENTE"]).issubset(set(cols_min)):
        print(" clientes_tratado.csv não tem CD_CLIENTE/DS_PROD suficientes para recomendação.")
        return

    # Map: cliente -> set de produtos atuais
    prods_cliente = (clientes[cols_min]
                     .dropna()
                     .groupby("CD_CLIENTE")["DS_PROD"]
                     .apply(lambda s: set(map(str, s)))
                     .to_dict())

    # Juntar com clusters_clientes.csv
    clusters = pd.read_csv("clusters_clientes.csv", encoding="utf-8")
    clientes_cluster = clusters.merge(clientes[["CD_CLIENTE","DS_PROD"]].dropna(), on="CD_CLIENTE", how="left")

    # TOP produtos por cluster
    top_produtos = (clientes_cluster.groupby(["cluster","DS_PROD"])["CD_CLIENTE"]
                    .count().reset_index().rename(columns={"CD_CLIENTE":"QTD"}))
    top_produtos = top_produtos.sort_values(["cluster","QTD"], ascending=[True, False])

    # Salvar tabela de top produtos por cluster
    top_produtos.to_csv("recomendacoes_por_cluster.csv", index=False, encoding="utf-8")
    print(" recomendacoes_por_cluster.csv salvo.")

    # Para cada cliente, recomendar TOP-N do cluster que ele não possui
    TOP_N = 3
    recs = []
    for _, row in clusters.iterrows():
        cid = row["CD_CLIENTE"]
        cl  = row["cluster"]
        ja_tem = prods_cliente.get(cid, set())

        top_do_cluster = top_produtos[top_produtos["cluster"] == cl]["DS_PROD"].tolist()
        sugerir = [p for p in top_do_cluster if p not in ja_tem][:TOP_N]
        recs.append({"CD_CLIENTE": cid, "cluster": cl, "RECOMENDACOES": ", ".join(map(str, sugerir))})

    pd.DataFrame(recs).to_csv("recomendacoes_por_cliente.csv", index=False, encoding="utf-8")
    print(" recomendacoes_por_cliente.csv salvo.")

# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    base = carregar_ou_construir_base()
    df, X, X_scaled, feat_names = preparar_features(base)

    # Treinar KMeans (k selecionado por silhouette)
    modelo = treinar_kmeans(X_scaled, ks=[3,4,5,6], random_state=42)
    labels = modelo.predict(X_scaled)

    # Salvar clusters e resumo
    # X aqui é DataFrame; feat_names é apenas informativo
    if isinstance(X, pd.DataFrame):
        salvar_resultados(df, X, labels, feat_names)
    else:
        # caso raro, mas garantimos uma estrutura DataFrame
        X_df = pd.DataFrame(X, columns=feat_names)
        salvar_resultados(df, X_df, labels, feat_names)

    # Recomendações
    gerar_recomendacoes(labels)

    print(" Pipeline de clusterização + recomendações concluído.")
