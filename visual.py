# -*- coding: utf-8 -*-

import os
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# --------------------------
# Args
# --------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Meraki Match – Visuais Finais")
    p.add_argument("--upload", action="store_true", help="Faz upload dos artefatos para S3")
    p.add_argument("--bucket", default="", help="Bucket S3 (necessário se --upload)")
    p.add_argument("--prefix", default="outputs/", help="Prefixo/pasta no S3 (padrão: outputs/)")
    p.add_argument("--aws-key", default=os.environ.get("AWS_ACCESS_KEY_ID", ""), help="AWS Access Key (ou variável de ambiente)")
    p.add_argument("--aws-secret", default=os.environ.get("AWS_SECRET_ACCESS_KEY", ""), help="AWS Secret Key (ou variável de ambiente)")
    p.add_argument("--topn", type=int, default=8, help="Qtde de features com maior variância para o gráfico (padrão=8)")
    return p.parse_args()

# --------------------------
# Helpers
# --------------------------
def safe_read_csv(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")
    return pd.read_csv(path, encoding="utf-8")

def to_numeric_br(series: pd.Series) -> pd.Series:
    s = (
        series.astype(str)
              .str.replace(r"\s+", "", regex=True)
              .str.replace(".", "", regex=False)   # remove separador de milhar
              .str.replace(",", ".", regex=False)  # vírgula -> ponto
    )
    return pd.to_numeric(s, errors="coerce")

def ensure_cd_cliente(df: pd.DataFrame) -> pd.DataFrame:
    poss = ["CD_CLIENTE", "CLIENTE", "IdCliente", "ID_CLIENTE", "COD_CLIENTE", "CODIGO_CLIENTE", "CD_CLI", "metadata_codcliente"]
    hit = [c for c in poss if c in df.columns]
    if hit and "CD_CLIENTE" not in df.columns:
        df = df.copy()
        df["CD_CLIENTE"] = df[hit[0]]
    return df

# --------------------------
# 1) Gráficos para PPT
# --------------------------
def grafico_distribuicao_clusters(clusters_csv="clusters_clientes.csv", saida_png="cluster_sizes.png"):
    df = safe_read_csv(clusters_csv)
    if "cluster" not in df.columns:
        raise ValueError("clusters_clientes.csv precisa ter a coluna 'cluster'.")
    sizes = df["cluster"].value_counts().sort_index()
    plt.figure()
    sizes.plot(kind="bar")
    plt.title("Distribuição de Clientes por Cluster")
    plt.xlabel("Cluster")
    plt.ylabel("Quantidade de Clientes")
    plt.tight_layout()
    plt.savefig(saida_png, dpi=200)
    plt.close()
    print(f"✅ {saida_png} gerado.")

def grafico_medias_features(cluster_summary_xlsx="cluster_summary.xlsx",
                            base_csv="base_analitica_meraki.csv",
                            clusters_csv="clusters_clientes.csv",
                            saida_png="cluster_feature_means.png",
                            topn=8):
    """
    Prioriza 'cluster_summary.xlsx' (sheet 'metricas_medias').
    Se não existir, calcula médias a partir de base + clusters (numéricas).
    """
    perfil = None
    if os.path.exists(cluster_summary_xlsx):
        try:
            perfil = pd.read_excel(cluster_summary_xlsx, sheet_name="metricas_medias")
            if "cluster" not in perfil.columns:
                raise ValueError("A aba 'metricas_medias' precisa ter a coluna 'cluster'.")
            perfil = perfil.set_index("cluster")
        except Exception as e:
            print(f"⚠️ Falha ao ler metricas_medias do {cluster_summary_xlsx}: {e}")

    if perfil is None:
        print("⚠️ Recalculando perfil de features a partir de base + clusters (fallback).")
        base = safe_read_csv(base_csv)
        clusters = safe_read_csv(clusters_csv)
        base = ensure_cd_cliente(base)
        df = base.merge(clusters[["CD_CLIENTE","cluster"]], on="CD_CLIENTE", how="left")
        # escolhe colunas numéricas
        num_cols = []
        for c in df.columns:
            if c in ("CD_CLIENTE","cluster"):
                continue
            # tenta converter para numérico
            s = to_numeric_br(df[c]) if df[c].dtype == "O" else pd.to_numeric(df[c], errors="coerce")
            if s.notna().sum() > 0:
                df[c] = s
                num_cols.append(c)
        perfil = df.groupby("cluster")[num_cols].mean(numeric_only=True)

    # Seleciona top N features por variância entre clusters
    variancias = perfil.var().sort_values(ascending=False)
    features_top = variancias.head(topn).index.tolist() if len(variancias) > 0 else perfil.columns.tolist()

    ax = perfil[features_top].plot(kind="bar", figsize=(10,5))
    ax.set_title("Médias das Principais Features por Cluster")
    ax.set_xlabel("Cluster")
    ax.set_ylabel("Média")
    plt.tight_layout()
    plt.savefig(saida_png, dpi=200)
    plt.close()
    print(f"✅ {saida_png} gerado.")

def planilha_top_produtos(recs_cluster_csv="recomendacoes_por_cluster.csv",
                          saida_xlsx="top_produtos_por_cluster.xlsx"):
    df = safe_read_csv(recs_cluster_csv)
    # Normaliza e ordena
    if "cluster" not in df.columns or "DS_PROD" not in df.columns:
        raise ValueError("recomendacoes_por_cluster.csv precisa ter colunas 'cluster' e 'DS_PROD'.")
    if "QTD" in df.columns:
        df["QTD"] = pd.to_numeric(df["QTD"], errors="coerce")
    df = df.sort_values(["cluster", "QTD"], ascending=[True, False])
    with pd.ExcelWriter(saida_xlsx, engine="xlsxwriter") as xlw:
        df.to_excel(xlw, index=False, sheet_name="top_produtos")
    print(f"✅ {saida_xlsx} gerado.")

# --------------------------
# 2) Rótulos legíveis (personas)
# --------------------------
def gerar_rotulos_clusters(base_csv="base_analitica_meraki.csv",
                           clusters_csv="clusters_clientes.csv",
                           saida_csv="clusters_rotulados.csv"):
    base = safe_read_csv(base_csv)
    clusters = safe_read_csv(clusters_csv)
    base = ensure_cd_cliente(base)

    df = base.merge(clusters, on="CD_CLIENTE", how="left")
    cols = [c for c in ["MRR_12M","NPS_MEDIO","QTD_CONTRATACOES_12M","VLR_CONTRATACOES_12M","VL_TOTAL_CONTRATO"] if c in df.columns]

    for c in cols:
        df[c] = to_numeric_br(df[c]) if df[c].dtype == "O" else pd.to_numeric(df[c], errors="coerce")

    perfil = df.groupby("cluster")[cols].mean(numeric_only=True).reset_index()

    def tercil(series):
        return series.quantile([0.33, 0.67]).values

    thresholds = {c: tercil(perfil[c]) for c in cols if c in perfil.columns}

    def bucket(v, q33, q67):
        if np.isnan(v): return "Médio"
        if v <= q33:    return "Baixo"
        if v >= q67:    return "Alto"
        return "Médio"

    def rotular(row):
        parts = []
        if "MRR_12M" in cols:
            q1,q2 = thresholds["MRR_12M"]; parts.append(f"Receita {bucket(row['MRR_12M'], q1, q2)}")
        if "NPS_MEDIO" in cols:
            q1,q2 = thresholds["NPS_MEDIO"]; parts.append(f"Satisfação {bucket(row['NPS_MEDIO'], q1, q2)}")
        if "QTD_CONTRATACOES_12M" in cols:
            q1,q2 = thresholds["QTD_CONTRATACOES_12M"]; parts.append(f"Aquisição {bucket(row['QTD_CONTRATACOES_12M'], q1, q2)}")
        return ", ".join(parts[:3])

    perfil["cluster_label"] = perfil.apply(rotular, axis=1)
    out = df.merge(perfil[["cluster","cluster_label"]], on="cluster", how="left")
    out[["CD_CLIENTE","cluster","cluster_label"]].to_csv(saida_csv, index=False, encoding="utf-8")
    print(f"✅ {saida_csv} gerado.")

# --------------------------
# 3) Upload opcional para S3
# --------------------------
def upload_s3(arquivos, bucket, prefix, aws_key, aws_secret):
    import boto3
    s3 = boto3.client("s3",
        aws_access_key_id=aws_key if aws_key else None,
        aws_secret_access_key=aws_secret if aws_secret else None,
    )
    for f in arquivos:
        if os.path.exists(f):
            s3.upload_file(f, bucket, prefix + f)
            print(f"☁️  Enviado: s3://{bucket}/{prefix}{f}")
        else:
            print(f"⚠️ Não encontrado (skip): {f}")

# --------------------------
# MAIN
# --------------------------
def main():
    args = parse_args()

    # Gráficos
    grafico_distribuicao_clusters()
    grafico_medias_features(topn=args.topn)
    planilha_top_produtos()

    # Rótulos legíveis
    gerar_rotulos_clusters()

    # Upload opcional
    if args.upload:
        if not args.bucket:
            raise ValueError("Para --upload, informe --bucket.")
        arquivos = [
            "cluster_sizes.png",
            "cluster_feature_means.png",
            "top_produtos_por_cluster.xlsx",
            "clusters_rotulados.csv",
        ]
        upload_s3(arquivos, args.bucket, args.prefix, args.aws_key, args.aws_secret)

    print(" Visuais finais concluídos.")

# Gráficos

def grafico_nps_medio_por_cluster(base_csv="base_analitica_meraki.csv",
                                  clusters_csv="clusters_clientes.csv",
                                  saida_png="nps_por_cluster.png"):
    base = safe_read_csv(base_csv)
    clusters = safe_read_csv(clusters_csv)
    base = ensure_cd_cliente(base)
    df = base.merge(clusters, on="CD_CLIENTE", how="left")
    if "NPS_MEDIO" not in df.columns:
        print(" NPS_MEDIO não encontrado em base_analitica_meraki.csv")
        return
    df["NPS_MEDIO"] = to_numeric_br(df["NPS_MEDIO"]) if df["NPS_MEDIO"].dtype == "O" else pd.to_numeric(df["NPS_MEDIO"], errors="coerce")
    nps = df.groupby("cluster", as_index=False)["NPS_MEDIO"].mean()

    import matplotlib.pyplot as plt
    plt.figure()
    plt.bar(nps["cluster"].astype(str), nps["NPS_MEDIO"])
    plt.axhline(0, linestyle="--", linewidth=1)
    plt.title("NPS Médio por Cluster")
    plt.xlabel("Cluster"); plt.ylabel("NPS Médio")
    plt.tight_layout(); plt.savefig(saida_png, dpi=200); plt.close()
    print(f"✅ {saida_png} gerado.")

def grafico_boxplot_mrr_por_cluster(base_csv="base_analitica_meraki.csv",
                                    clusters_csv="clusters_clientes.csv",
                                    saida_png="mrr_boxplot_por_cluster.png"):
    base = safe_read_csv(base_csv)
    clusters = safe_read_csv(clusters_csv)
    base = ensure_cd_cliente(base)
    df = base.merge(clusters, on="CD_CLIENTE", how="left")
    if "MRR_12M" not in df.columns:
        print("⚠️ MRR_12M não encontrado em base_analitica_meraki.csv")
        return
    df["MRR_12M"] = to_numeric_br(df["MRR_12M"]) if df["MRR_12M"].dtype == "O" else pd.to_numeric(df["MRR_12M"], errors="coerce")

    grupos = []
    labels = []
    for cl in sorted(df["cluster"].dropna().unique()):
        serie = df.loc[df["cluster"]==cl, "MRR_12M"].dropna()
        if len(serie) > 0:
            grupos.append(serie.values)
            labels.append(str(cl))
    if not grupos:
        print("⚠️ Sem dados suficientes para boxplot de MRR.")
        return

    import matplotlib.pyplot as plt
    plt.figure()
    plt.boxplot(grupos, labels=labels, showfliers=False)
    plt.title("Distribuição de MRR por Cluster (Boxplot)")
    plt.xlabel("Cluster"); plt.ylabel("MRR (12M)")
    plt.tight_layout(); plt.savefig(saida_png, dpi=200); plt.close()
    print(f"✅ {saida_png} gerado.")

def grafico_composicao_segmento(base_csv="base_analitica_meraki.csv",
                                clusters_csv="clusters_clientes.csv",
                                saida_png="segmento_stack_por_cluster.png"):
    base = safe_read_csv(base_csv)
    clusters = safe_read_csv(clusters_csv)
    base = ensure_cd_cliente(base)
    df = base.merge(clusters, on="CD_CLIENTE", how="left")

    if "DS_SEGMENTO" not in df.columns:
        print("⚠️ DS_SEGMENTO não encontrado em base_analitica_meraki.csv")
        return

    tab = pd.crosstab(df["cluster"], df["DS_SEGMENTO"])
    if tab.empty:
        print("⚠️ Tabela vazia para composição por segmento.")
        return
    prop = tab.div(tab.sum(axis=1), axis=0)  # percentuais por cluster

    import matplotlib.pyplot as plt
    plt.figure(figsize=(10,6))
    bottom = np.zeros(len(prop))
    x = np.arange(len(prop.index))
    for seg in prop.columns:
        plt.bar(x, prop[seg].values, bottom=bottom, label=str(seg))
        bottom += prop[seg].values
    plt.title("Composição por Segmento (%) — por Cluster")
    plt.xlabel("Cluster"); plt.ylabel("% dentro do Cluster")
    plt.xticks(x, prop.index.astype(str))
    plt.legend(loc="best", ncol=2, fontsize=8)
    plt.tight_layout(); plt.savefig(saida_png, dpi=200); plt.close()
    print(f"✅ {saida_png} gerado.")

def grafico_top_produtos_por_cluster(recs_cluster_csv="recomendacoes_por_cluster.csv",
                                     saida_dir="top_produtos_imgs",
                                     top_n=10):
    df = safe_read_csv(recs_cluster_csv)
    if not set(["cluster","DS_PROD"]).issubset(df.columns):
        print("⚠️ recomendacoes_por_cluster.csv precisa ter 'cluster' e 'DS_PROD'.")
        return
    if "QTD" in df.columns:
        df["QTD"] = pd.to_numeric(df["QTD"], errors="coerce").fillna(0)
    os.makedirs(saida_dir, exist_ok=True)

    for cl in sorted(df["cluster"].unique()):
        top = df[df["cluster"]==cl].sort_values("QTD", ascending=False).head(top_n)
        if top.empty:
            continue
        import matplotlib.pyplot as plt
        plt.figure(figsize=(8,6))
        plt.barh(top["DS_PROD"].astype(str)[::-1], top["QTD"].values[::-1])
        plt.title(f"Top {top_n} Produtos — Cluster {cl}")
        plt.xlabel("Quantidade"); plt.ylabel("Produto")
        plt.tight_layout()
        out = os.path.join(saida_dir, f"top_produtos_cluster_{cl}.png")
        plt.savefig(out, dpi=200); plt.close()
        print(f"✅ {out} gerado.")

# ---- EXEMPLO: chamar os extras logo após o main do seu script ----
if __name__ == "__main__":
    # (se você já tem o main acima, pode apenas chamar os extras aqui embaixo)
    try:
        grafico_nps_medio_por_cluster()
    except Exception as e:
        print(f"extras: NPS por cluster falhou: {e}")
    try:
        grafico_boxplot_mrr_por_cluster()
    except Exception as e:
        print(f"extras: boxplot MRR falhou: {e}")
    try:
        grafico_composicao_segmento()
    except Exception as e:
        print(f"extras: composição por segmento falhou: {e}")
    try:
        grafico_top_produtos_por_cluster()
    except Exception as e:
        print(f"extras: top produtos falhou: {e}")


if __name__ == "__main__":
    main()
