# Case de Análise de Dados iFood - Analytics Engineer

## 1. Visão Geral do Projeto

Este repositório contém a solução para o case prático do processo seletivo de Analytics Engineer na iFood. O objetivo principal do projeto é analisar os resultados de um teste A/B, construindo um pipeline de dados  na plataforma Databricks. A solução abrange desde a ingestão e tratamento de dados brutos até a análise estatística e segmentação de clientes para geração de insights acionáveis.

O projeto foi desenvolvido utilizando **PySpark** e **SQL** em um ambiente Databricks, seguindo as melhores práticas de engenharia de dados, como a implementação da **Arquitetura Medallion** e a utilização do **Unity Catalog** para governança de dados.

## 2. Arquitetura da Solução

A solução foi implementada seguindo a Arquitetura Medallion, que organiza os dados em camadas lógicas de qualidade crescente: Bronze, Silver e Gold.

* **Camada Bronze (Bruta):**
    * **Propósito:** Ingestão dos dados brutos de diversas fontes (JSON, CSV) sem transformação.
    * **Ações:** Download dos arquivos, criação de volumes no Unity Catalog e carregamento dos dados em tabelas Spark com schema explícito para garantir a consistência inicial.

* **Camada Silver (Validada e Limpa):**
    * **Propósito:** Fornecer uma visão limpa, validada e enriquecida dos dados. 
    * **Ações:**
        * **Validação de Qualidade:** Verificação de chaves primárias, unicidade, integridade referencial e regras de negócio (ex: valores de pedidos não negativos).
        * **Limpeza de Dados:** Aplicação de estratégias de deduplicação (ex: manter o pedido mais recente em caso de `order_id` duplicado).
        * **Engenharia de Features:** Desaninhamento (parsing) de colunas complexas (como o JSON de `items`), e criação de novas variáveis relevantes para a análise (ex: `preco_medio_por_item`, `item_mais_caro_no_carrinho`, etc.).
        * **Construção da ABT (Analytical Base Table):** Consolidação dos dados de pedidos, consumidores, restaurantes e grupos do teste A/B em uma única tabela, enriquecida com features de contexto e histórico do cliente (ex: `dias_desde_ultimo_pedido`, `gasto_medio_historico_cliente`).

* **Camada Gold (Agregada e de Negócio):**
    * **Propósito:** Fornecer dados agregados e KPIs (Key Performance Indicators) prontos para consumo por stakeholders, dashboards ou análises estatísticas.
    * **Ações:**
        * **Cálculo de KPIs:** Agregação de métricas por usuário (ex: receita total, frequência, ticket médio, taxas de retenção).
        * **Análise Estatística:** Execução de testes de significância (T-tests) para comparar o desempenho entre os grupos de controle e alvo.
        * **Análise por Segmento:** Criação de segmentos de ciclo de vida do cliente (ex: `new`, `active`, `at_risk`) para entender como o experimento impactou diferentes perfis de usuários.

## 3. Estrutura do Código (`Case_Ifood.py`)

O script Python anexo é uma exportação de um notebook Databricks. Ele está organizado em seções lógicas que correspondem às etapas do pipeline de dados.

#### **Seção 1: Configuração e Camada Bronze**
* Define os nomes do catálogo e schema no Unity Catalog.
* Cria um volume para armazenar os arquivos brutos.
* Utiliza um comando `%sh` para baixar os arquivos-fonte da AWS S3 e descompactar o arquivo `tar.gz`, garantindo a idempotência (só baixa se os arquivos não existirem).
* Define schemas explícitos (`StructType`) para cada fonte de dados, uma prática essencial para evitar inferência de schema e garantir a robustez do pipeline.
* Ingere os dados brutos nas tabelas da camada Bronze.

#### **Seção 2: Validação e Preparação (Camada Silver)**
* **Verificações de Qualidade:** Utiliza `asserts` para garantir a integridade dos dados, como a ausência de chaves primárias nulas e a unicidade dos identificadores após a limpeza.
* **Deduplicação Inteligente:** Identifica `order_id` duplicados e aplica uma função de janela (`Window`) para manter apenas o registro mais recente de cada pedido, resolvendo uma inconsistência crítica nos dados.
* **Engenharia de Features em `orders`:**
    * Realiza o parsing da complexa coluna `items` (JSON) para extrair informações detalhadas de cada produto no carrinho.
    * Cria features como quantidade de itens, preço médio, desvio padrão dos preços e o valor do item mais caro.
    * Utiliza uma UDF (User-Defined Function) com `numpy` para calcular o desvio padrão dos preços dos itens, demonstrando a capacidade de estender as funcionalidades do Spark.

#### **Seção 3: Construção da Tabela Analítica (ABT)**
* Seleciona as colunas mais relevantes de cada tabela Bronze.
* **Consolidação:** Realiza `joins` entre as tabelas de pedidos, consumidores, restaurantes e usuários do teste A/B para criar uma tabela única e ampla.
* **Enriquecimento com Funções de Janela:** Cria features avançadas que fornecem contexto sobre o comportamento do cliente:
    * `dias_desde_ultimo_pedido`: Calcula a recorrência.
    * `gasto_medio_historico_cliente`: Modela o comportamento de gastos passado.
    * `frequencia_cliente_no_restaurante`: Identifica a lealdade do cliente a um restaurante específico.

#### **Seção 4: Análises e KPIs (Camada Gold)**
* **Cálculo de Métricas por Usuário:** Agrega os dados da ABT para calcular KPIs fundamentais como receita total (LTV), frequência, ticket médio e taxas de retenção (7 e 14 dias).
* **Criação da Tabela de Sumário:** Agrupa os KPIs por grupo do teste A/B (`control` vs `target`), calculando médias e desvios padrão. Esta tabela é a base para a análise dos resultados.
* **Testes de Significância Estatística:**
    * Utiliza a biblioteca `scipy.stats` para executar T-tests.
    * Compara as médias das principais métricas entre os grupos para determinar se as diferenças observadas são estatisticamente significativas (`p-value < 0.05`).
    * Calcula o `lift` (aumento percentual) para quantificar o impacto da mudança testada.

#### **Seção 5: Análise Segmentada**
* **Segmentação de Clientes:** Classifica os usuários em segmentos de ciclo de vida (`new`, `active`, `at_risk`) com base em seu histórico de pedidos.
* **Análise A/B por Segmento:** Re-calcula os KPIs e executa os T-tests para cada segmento, permitindo uma análise mais profunda para entender se o experimento teve um impacto diferente em novos clientes versus clientes já ativos, por exemplo.

## 4. Como Executar o Código

Para executá-lo, o ambiente ideal é um notebook Databricks.

#### **Pré-requisitos:**
1.  **Ambiente Databricks:** Um workspace Databricks com acesso ao Unity Catalog.
2.  **Cluster:** Um cluster Databricks em execução.
    * **Versão do Databricks Runtime:** Recomenda-se uma versão recente (ex: 13.x ou superior).
    * **Bibliotecas:** O script utiliza `pyspark`, `numpy`, `pandas` e `scipy`.

#### **Passos para Execução:**
1.  **Faça o Upload do Script:** Faça o upload do arquivo `Case_Ifood.py` para o seu workspace Databricks.
2.  **Crie um Novo Notebook:** Crie um novo notebook em branco no seu workspace.
3.  **Copie o Conteúdo do Script:** Abra o arquivo `Case_Ifood.py` no editor de texto do Databricks, copie seu conteúdo e cole-o em uma célula.
4.  **Anexe o Cluster:** Anexe o notebook ao cluster que você configurou.
5.  **Execute as Células:** Execute as células do notebook em sequência, do início ao fim. As saídas, incluindo os DataFrames e os resultados dos testes estatísticos, serão exibidas abaixo de cada célula correspondente.

## 5. Principais Insights e Resultados

A análise dos resultados do teste A/B revelou insights  sobre o impacto da campanha de cupons, validando e refutando partes da hipótese inicial. Os dados mostram que, embora a campanha tenha sido bem-sucedida em métricas gerais, seu verdadeiro efeito só é compreendido através da segmentação.

#### Análise Geral (Visão Agregada):
* **Sucesso no Engajamento e Retenção:** A campanha foi eficaz em aumentar a **Receita por Cliente (+13,1%)**, a **Frequência de Pedidos (+13,3%)** e, crucialmente, as **Taxas de Retenção de 7 e 14 dias (+20,8% e +20,9%, respectivamente)**. Isso confirma que o cupom funciona para ativar usuários e gerar lealdade a curto prazo.
* **Comportamento de Compra Inalterado:** O **Ticket Médio** não apresentou mudança estatisticamente significativa. O ganho de receita veio exclusivamente de mais pedidos, não de pedidos de maior valor.
* **Sinais de Alerta:** Foram identificados dois efeitos colaterais negativos:
    * Um aumento de **21,2% no Tempo Médio para Recompra**, sugerindo que os usuários podem estar aguardando por novos descontos para comprar novamente.
    * Uma queda de **2,9% na Taxa de Pedidos em Novos Restaurantes**, indicando que o cupom pode prejudicar a exploração e a saúde do ecossistema a longo prazo.

#### Análise por Segmento (Visão Aprofundada):
* **Novos Usuários (Poderosa Ferramenta de Ativação):**
    * O cupom gerou um aumento massivo de **+40,8% na Retenção de 14 dias** e de **+5,5% na Frequência** para este grupo.
    * Confirma-se que o cupom é uma ferramenta de **ativação** extremamente eficaz para garantir a segunda compra.

* **Usuários Ativos (Ineficaz e Prejudicial):**
    * **Nenhuma métrica apresentou mudança significativa**. O cupom não alterou o comportamento de compra de quem já era fiel.
    * A conclusão é que a campanha **canibalizou a receita** neste segmento, oferecendo descontos para compras que já iriam acontecer. Este grupo foi o principal responsável pelo ROI negativo da campanha original.

* **Usuários em Risco:**
    * O volume de dados para este segmento foi insuficiente para tirar conclusões estatísticas, representando uma lacuna de conhecimento e uma oportunidade para testes futuros.

## 6. Recomendações e Plano de Ação Estratégico

A análise aponta para uma direção clara: devemos abandonar a abordagem de "um cupom para todos" e evoluir para um portfólio de incentivos inteligente e segmentado.

O plano de ação se desdobra em três frentes estratégicas:

1.  **Ativação de Novos Clientes (Roll-out Imediato):**
    * **Ação:** Implementar a campanha de cupons de forma contínua e automatizada, direcionada exclusivamente para novos usuários após a primeira compra.
    * **Justificativa:** O lift de **+41% na retenção inicial** valida o ROI para este segmento, otimizando o Custo de Aquisição de Cliente (CAC).

2.  **Rentabilização da Base Ativa (Descontinuar e Substituir):**
    * **Ação:** Descontinuar a oferta de cupons de desconto para usuários ativos e fiéis.
    * **Justificativa:** A comprovada canibalização de receita. Em vez de descontos, o foco para este grupo deve ser em programas de fidelidade e recompensas que reforcem a lealdade sem erodir a margem.

3.  **Reativação de Clientes em Risco (Novo Ciclo de Experimentação):**
    * **Ação:** Iniciar um novo ciclo de testes A/B focado especificamente no segmento "Em Risco" para preencher a lacuna de dados atual.
    * **Justificativa:** A hipótese é que um incentivo no momento certo pode reativar esses clientes antes do churn. O próximo teste deve avaliar diferentes tipos de ofertas (ex: cupom de valor fixo, frete grátis, cupom para categorias específicas) para encontrar a alavanca mais eficaz para este grupo.
```
