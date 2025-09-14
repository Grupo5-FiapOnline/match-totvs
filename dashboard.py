# app.py
import streamlit as st
import pandas as pd

PASTA = "./assets/"

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Dashboard de Análise de Clientes",
    page_icon="📊",
    layout="wide"
)

# --- FUNÇÃO DE CARREGAMENTO E TRATAMENTO DOS DADOS ---


@st.cache_data  # Cache para carregar os dados apenas uma vez
def carregar_dados():
    # Caminhos para os arquivos (devem estar na mesma pasta que o app.py)
    caminho_clientes = f"{PASTA}clientes_tratado.csv"
    caminho_tickets = f"{PASTA}tickets_agg_organizacao.csv"
    caminho_vendas = f"{PASTA}vendas_tratado.csv"

    # --- Carregamento ---
    try:
        df_clientes = pd.read_csv(caminho_clientes)
        df_tickets = pd.read_csv(caminho_tickets)
        df_vendas = pd.read_csv(caminho_vendas)
    except FileNotFoundError as e:
        st.error(
            f"Erro: Arquivo não encontrado. Verifique se os arquivos CSV estão na mesma pasta que o 'app.py'. Detalhe: {e}")
        return None

    # --- Tratamento e Limpeza ---

    # 1. Clientes: Para a visão geral, vamos usar uma linha por cliente, removendo duplicatas.
    #    Vamos manter a primeira ocorrência de cada cliente.
    df_clientes_unico = df_clientes.drop_duplicates(
        subset='CD_CLIENTE', keep='first')

    # 2. Vendas: A coluna 'MRR_12M' parece estar correta.
    #    Vamos garantir que o ID do cliente seja do mesmo tipo para o merge.
    df_vendas['CLIENTE'] = df_vendas['CLIENTE'].astype(str)

    # 3. Tickets: O ID está como 'CODIGO_ORGANIZACAO'. Vamos renomear para facilitar o merge.
    df_tickets = df_tickets.rename(
        columns={'CODIGO_ORGANIZACAO': 'CD_CLIENTE'})
    df_tickets['CD_CLIENTE'] = df_tickets['CD_CLIENTE'].astype(str)

    # --- Merge dos Dados ---
    # Começamos com a base de clientes únicos
    df_master = df_clientes_unico[['CD_CLIENTE',
                                   'CIDADE', 'DS_SEGMENTO', 'UF']].copy()
    df_master['CD_CLIENTE'] = df_master['CD_CLIENTE'].astype(str)

    # Adicionamos os dados de Vendas (left join para manter todos os clientes)
    df_master = pd.merge(
        df_master,
        df_vendas[['CLIENTE', 'MRR_12M']],
        left_on='CD_CLIENTE',
        right_on='CLIENTE',
        how='left'
    )
    # Remove a coluna duplicada do ID
    df_master = df_master.drop(columns=['CLIENTE'])

    # Adicionamos os dados de Tickets (left join para manter todos os clientes)
    df_master = pd.merge(
        df_master,
        df_tickets[['CD_CLIENTE', 'QTD_CHAMADOS']],
        on='CD_CLIENTE',
        how='left'
    )

    # Preenche valores nulos que podem ter surgido do merge
    df_master['MRR_12M'] = df_master['MRR_12M'].fillna(0)
    df_master['QTD_CHAMADOS'] = df_master['QTD_CHAMADOS'].fillna(0).astype(int)

    return df_master


# --- INÍCIO DA EXECUÇÃO DO DASHBOARD ---
st.title("📊 Dashboard de Análise de Clientes 360°")
st.markdown(
    "Análise da relação entre perfil do cliente, vendas e tickets de suporte.")

# Carrega os dados usando a função
df_master = carregar_dados()

if df_master is not None:
    # --- FILTROS NA BARRA LATERAL (SIDEBAR) ---
    st.sidebar.header("Filtros da Análise")

    # Filtro por UF
    ufs_unicas = sorted(df_master['UF'].dropna().unique())
    uf_selecionada = st.sidebar.multiselect(
        "Filtrar por UF:", ufs_unicas, default=ufs_unicas)

    # Filtro por Segmento
    segmentos_unicos = sorted(df_master['DS_SEGMENTO'].dropna().unique())
    segmento_selecionado = st.sidebar.multiselect(
        "Filtrar por Segmento:", segmentos_unicos, default=segmentos_unicos)

    # Aplica os filtros ao dataframe
    df_filtrado = df_master[
        df_master['UF'].isin(uf_selecionada) &
        df_master['DS_SEGMENTO'].isin(segmento_selecionado)
    ]

    # --- EXIBIÇÃO DOS KPIs (Métricas Principais) ---
    st.markdown("---")
    st.header("Visão Geral")

    total_clientes = df_filtrado['CD_CLIENTE'].nunique()
    receita_total = df_filtrado['MRR_12M'].sum()
    total_chamados = df_filtrado['QTD_CHAMADOS'].sum()

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total de Clientes", f"{total_clientes:,}".replace(",", "."))
    with col2:
        st.metric("Receita MRR Total", f"R$ {receita_total:,.2f}".replace(
            ",", "X").replace(".", ",").replace("X", "."))
    with col3:
        st.metric("Total de Chamados", f"{total_chamados:,}".replace(",", "."))

    # --- GRÁFICOS ---
    st.markdown("---")
    st.header("Análises Visuais")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader(f"Top Cidades por Nº de Clientes")
        clientes_por_cidade = df_filtrado.groupby(
            'CIDADE')['CD_CLIENTE'].nunique().nlargest(10).sort_values(ascending=True)
        st.bar_chart(clientes_por_cidade, height=400)

    with col2:
        st.subheader(f"Receita MRR por Segmento")
        receita_por_segmento = df_filtrado.groupby(
            'DS_SEGMENTO')['MRR_12M'].sum().sort_values(ascending=True)
        st.bar_chart(receita_por_segmento, height=400)

    # --- TABELA DE DADOS ---
    st.markdown("---")
    st.header("Dados Detalhados")
    st.dataframe(df_filtrado)
