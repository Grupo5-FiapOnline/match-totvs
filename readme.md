# Meraki Match

## Sobre o Projeto

Meraki Match é um sistema de recomendação inteligente desenvolvido para a TOTVS com o objetivo de potencializar a jornada do cliente. Utilizando técnicas de ciência de dados e machine learning, a solução clusteriza os clientes da TOTVS para identificar padrões de comportamento e uso, gerando recomendações personalizadas de produtos e serviços.

O nome "Meraki" é uma palavra de origem grega que significa fazer algo com alma, criatividade e amor, colocando um pedaço de si mesmo no trabalho.

Este projeto foi desenvolvido pelos alunos da turma 1TSCOA da FIAP.

### Equipe
* Cristiano Pires - RM557463
* Gianfranco Di Matteo - RM556662
* Guilherme Lourenço Sales - RM554506
* José Sérgio de Góis - RM555641
* Pedro Ivo de Paiva Moraes - RM557547

## Problema

A TOTVS, líder no setor de tecnologia e ERP no Brasil, oferece um vasto portfólio de soluções. No entanto, muitos clientes não exploram todo o potencial das ferramentas que contratam. O desafio é identificar, de forma automatizada e escalável, quais outros produtos do portfólio da TOTVS seriam mais aderentes a cada cliente, com base em seu perfil e padrões de uso.

## Solução Proposta

O Meraki Match aplica técnicas de ciência de dados para clusterizar os clientes com base em variáveis como segmento, utilização de produtos, engajamento, histórico de suporte, NPS e faturamento. A partir desses clusters, o sistema gera recomendações inteligentes de produtos e serviços que são utilizados com sucesso por clientes de perfil semelhante.

A solução consiste em:
* **Pipeline de dados:** Pré-processamento e padronização dos dados dos clientes.
* **Clusterização não supervisionada:** Agrupamento de clientes com comportamentos similares.
* **Sistema de recomendação:** Lógica baseada em "clientes que utilizam X também utilizam Y" dentro do mesmo cluster.
* **Dashboard interativo:** Visualização de insights por cluster, desenvolvido com Streamlit.

O público-alvo inicial são os clientes B2B da TOTVS dos segmentos de logística, varejo, manufatura e serviços que já possuem um relacionamento ativo e contrataram parte do portfólio.

## Arquitetura da Solução

O fluxo de dados e processamento da solução segue as seguintes etapas:
1.  **Ingestão de Dados:** Os dados dos clientes da TOTVS são disponibilizados em um bucket do Amazon S3.
2.  **ETL:** Um script em Python (`etl_s3_totvs.py`) consome os arquivos do S3, realiza o tratamento e a padronização, e consolida tudo em uma base analítica.
3.  **Clusterização e Recomendação:** O script `meraki_cluster_recomendacao.py` utiliza a base analítica para segmentar os clientes usando K-Means e gerar as recomendações de produtos.
4.  **Visualização de Dados:** São gerados artefatos visuais (`visual.py`), como gráficos e relatórios, para a análise dos resultados e apresentação executiva.

## Como Funciona

### 1. ETL (etl_s3_totvs.py)
O script é responsável por:
* Conectar ao AWS S3 e realizar a leitura de múltiplos arquivos CSV e XLSX, como dados de clientes, NPS, tickets de suporte, vendas (MRR) e telemetria.
* Realizar a limpeza e o pré-processamento dos dados, incluindo a unificação de chaves de clientes, normalização de campos numéricos e tratamento de dados faltantes.
* Consolidar todas as informações em uma única base analítica (`base_analitica_meraki.csv`).

### 2. Clusterização e Geração de Recomendações (meraki_cluster_recomendacao.py)
Este script executa as seguintes etapas:
* Carrega a base analítica consolidada.
* Aplica técnicas de engenharia de features, como a criação da variável `ANTIGUIDADE_MESES` e a aplicação de One-Hot Encoding em variáveis categóricas.
* Executa o algoritmo K-Means para clusterizar os clientes, testando diferentes números de clusters e selecionando o melhor valor com base no `silhouette score`.
* Gera as recomendações de produtos para cada cliente, identificando os produtos mais populares em seu respectivo cluster e sugerindo aqueles que o cliente ainda não possui.
* Salva as saídas em arquivos CSV e XLSX, incluindo a lista de clientes por cluster e as recomendações geradas.

### 3. Visualização (visual.py)
Este módulo é responsável por:
* Gerar visualizações gráficas a partir dos dados processados, como a distribuição de clientes por cluster, médias de features por cluster e perfis de cluster.
* Criar "personas" para cada cluster com base em métricas de receita, satisfação e aquisição.
* Exportar os resultados para serem consumidos pela área de negócios ou exibidos em dashboards.

## Tecnologias Utilizadas
* **Linguagem:** Python
* **Bibliotecas:** Pandas, NumPy, Scikit-learn, Boto3, Matplotlib, XlsxWriter, Six.
* **Cloud:** Amazon Web Services (AWS) S3.

## Impactos e Benefícios Esperados
* **Aumento de Receita:** Geração de oportunidades de vendas cruzadas (cross-sell) e upgrades.
* **Retenção de Clientes:** Aumento da satisfação e lealdade ao oferecer soluções que agregam valor de forma proativa.
* **Eficiência Comercial:** Fornecimento de inteligência de dados para direcionar a equipe de vendas.
* **Inovação no Atendimento:** Posicionamento da TOTVS como uma empresa orientada a dados, similar a grandes empresas de tecnologia.

## Agradecimentos
Gostaríamos de agradecer à FIAP pela oportunidade de desenvolver este projeto, aplicando conhecimentos teóricos em um desafio prático e relevante para o mercado.
