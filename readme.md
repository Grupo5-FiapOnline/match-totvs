# Meraki Match

## Sobre o Projeto

[cite_start]Meraki Match é um sistema de recomendação inteligente desenvolvido para a TOTVS com o objetivo de potencializar a jornada do cliente. [cite: 22] [cite_start]Utilizando técnicas de ciência de dados e machine learning, a solução clusteriza os clientes da TOTVS para identificar padrões de comportamento e uso, gerando recomendações personalizadas de produtos e serviços. [cite: 26, 33]

[cite_start]O nome "Meraki" é uma palavra de origem grega que significa fazer algo com alma, criatividade e amor, colocando um pedaço de si mesmo no trabalho. [cite: 1, 2]

[cite_start]Este projeto foi desenvolvido pelos alunos da turma 1TSCOA da FIAP. [cite: 4, 5]

### Equipe
* [cite_start]Cristiano Pires - RM557463 [cite: 8, 9]
* [cite_start]Gianfranco Di Matteo - RM556662 [cite: 10, 11]
* [cite_start]Guilherme Lourenço Sales - RM554506 [cite: 12, 13]
* [cite_start]José Sérgio de Góis - RM555641 [cite: 14, 15]
* [cite_start]Pedro Ivo de Paiva Moraes - RM557547 [cite: 16, 17]

## Problema

[cite_start]A TOTVS, líder no setor de tecnologia e ERP no Brasil, oferece um vasto portfólio de soluções. [cite: 20] [cite_start]No entanto, muitos clientes não exploram todo o potencial das ferramentas que contratam. [cite: 21] [cite_start]O desafio é identificar, de forma automatizada e escalável, quais outros produtos do portfólio da TOTVS seriam mais aderentes a cada cliente, com base em seu perfil e padrões de uso. [cite: 24]

## Solução Proposta

[cite_start]O Meraki Match aplica técnicas de ciência de dados para clusterizar os clientes com base em variáveis como segmento, utilização de produtos, engajamento, histórico de suporte, NPS e faturamento. [cite: 26, 27, 28, 29, 30, 31, 32] [cite_start]A partir desses clusters, o sistema gera recomendações inteligentes de produtos e serviços que são utilizados com sucesso por clientes de perfil semelhante. [cite: 33]

A solução consiste em:
* [cite_start]**Pipeline de dados:** Pré-processamento e padronização dos dados dos clientes. [cite: 43]
* [cite_start]**Clusterização não supervisionada:** Agrupamento de clientes com comportamentos similares. [cite: 44]
* [cite_start]**Sistema de recomendação:** Lógica baseada em "clientes que utilizam X também utilizam Y" dentro do mesmo cluster. [cite: 41, 45]
* [cite_start]**Dashboard interativo:** Visualização de insights por cluster, desenvolvido com Streamlit. [cite: 46]

[cite_start]O público-alvo inicial são os clientes B2B da TOTVS dos segmentos de logística, varejo, manufatura e serviços que já possuem um relacionamento ativo e contrataram parte do portfólio. [cite: 35, 36, 37, 39]

## Arquitetura da Solução

O fluxo de dados e processamento da solução segue as seguintes etapas:
1.  [cite_start]**Ingestão de Dados:** Os dados dos clientes da TOTVS são disponibilizados em um bucket do Amazon S3. [cite: 70]
2.  [cite_start]**ETL:** Um script em Python (`ETL_S3_TOTVS.PY`) consome os arquivos do S3, realiza o tratamento e a padronização, e consolida tudo em uma base analítica. [cite: 72, 87]
3.  [cite_start]**Clusterização e Recomendação:** O script `MERAKI_CLUSTER_RECOMENDAÇÃO.PY` utiliza a base analítica para segmentar os clientes usando K-Means e gerar as recomendações de produtos. [cite: 103, 125, 128]
4.  [cite_start]**Visualização de Dados:** São gerados artefatos visuais, como gráficos e relatórios, para a análise dos resultados e apresentação executiva. [cite: 135]

## Como Funciona

### 1. ETL (ETL_S3_TOTVS.PY)
O script é responsável por:
* [cite_start]Conectar ao AWS S3 e realizar a leitura de múltiplos arquivos CSV e XLSX, como dados de clientes, NPS, tickets de suporte, vendas (MRR) e telemetria. [cite: 72, 73, 88]
* [cite_start]Realizar a limpeza e o pré-processamento dos dados, incluindo a unificação de chaves de clientes, normalização de campos numéricos e tratamento de dados faltantes. [cite: 89, 90, 91]
* [cite_start]Consolidar todas as informações em uma única base analítica (`base_analitica_meraki.csv`). [cite: 85, 95]

### 2. Clusterização e Geração de Recomendações (MERAKI_CLUSTER_RECOMENDAÇÃO.PY)
Este script executa as seguintes etapas:
* [cite_start]Carrega a base analítica consolidada. [cite: 105, 118]
* [cite_start]Aplica técnicas de engenharia de features, como a criação da variável `ANTIGUIDADE_MESES` e a aplicação de One-Hot Encoding em variáveis categóricas. [cite: 121, 124]
* [cite_start]Executa o algoritmo K-Means para clusterizar os clientes, testando diferentes números de clusters e selecionando o melhor valor com base no `silhouette score`. [cite: 125, 126]
* [cite_start]Gera as recomendações de produtos para cada cliente, identificando os produtos mais populares em seu respectivo cluster e sugerindo aqueles que o cliente ainda não possui. [cite: 131]
* [cite_start]Salva as saídas em arquivos CSV e XLSX, incluindo a lista de clientes por cluster e as recomendações geradas. [cite: 109, 110, 113, 114]

### 3. Visualização (visual.py)
Este módulo é responsável por:
* [cite_start]Gerar visualizações gráficas a partir dos dados processados, como a distribuição de clientes por cluster, médias de features por cluster e perfis de cluster em gráficos de radar. [cite: 142, 143, 148]
* [cite_start]Criar "personas" para cada cluster com base em métricas de receita, satisfação e aquisição. [cite: 145, 149]
* [cite_start]Exportar os resultados para serem consumidos pela área de negócios ou exibidos em dashboards. [cite: 150]

## Tecnologias Utilizadas
* [cite_start]**Linguagem:** Python [cite: 70]
* [cite_start]**Bibliotecas:** Pandas, NumPy, Scikit-learn, Boto3, Streamlit. [cite: 70, 97, 124]
* [cite_start]**Cloud:** Amazon Web Services (AWS) S3. [cite: 70]

## Impactos e Benefícios Esperados
* [cite_start]**Aumento de Receita:** Geração de oportunidades de vendas cruzadas (cross-sell) e upgrades. [cite: 51, 58]
* [cite_start]**Retenção de Clientes:** Aumento da satisfação e lealdade ao oferecer soluções que agregam valor de forma proativa. [cite: 52, 60]
* [cite_start]**Eficiência Comercial:** Fornecimento de inteligência de dados para direcionar a equipe de vendas. [cite: 54, 62]
* [cite_start]**Inovação no Atendimento:** Posicionamento da TOTVS como uma empresa orientada a dados, similar a grandes empresas de tecnologia. [cite: 66, 67]

## Agradecimentos
[cite_start]Gostaríamos de agradecer à FIAP pela oportunidade de desenvolver este projeto, aplicando conhecimentos teóricos em um desafio prático e relevante para o mercado. [cite: 164, 165]
