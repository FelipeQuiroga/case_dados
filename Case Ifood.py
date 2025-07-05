# Databricks notebook source
# MAGIC %md
# MAGIC ###Inicio , volumes e catalogo
# MAGIC

# COMMAND ----------


CATALOG_NAME = "workspace"

SCHEMA_NAME = "bronze_data"

VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/raw_files"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.raw_files")

print(f"Volume path: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %sh
# MAGIC # Caminho do Volume e do arquivo 
# MAGIC VOLUME_PATH="/Volumes/workspace/bronze_data/raw_files"
# MAGIC FINAL_FILE_PATH="${VOLUME_PATH}/ab_test_ref.csv" 
# MAGIC
# MAGIC # URLs dos arquivos
# MAGIC URL_ORDER="https://data-architect-test-source.s3-sa-east-1.amazonaws.com/order.json.gz"
# MAGIC URL_CONSUMER="https://data-architect-test-source.s3-sa-east-1.amazonaws.com/consumer.csv.gz"
# MAGIC URL_RESTAURANT="https://data-architect-test-source.s3-sa-east-1.amazonaws.com/restaurant.csv.gz"
# MAGIC URL_AB_TEST="https://data-architect-test-source.s3-sa-east-1.amazonaws.com/ab_test_ref.tar.gz"
# MAGIC
# MAGIC
# MAGIC if [ ! -f "$FINAL_FILE_PATH" ]; then
# MAGIC     echo "Arquivo final '$FINAL_FILE_PATH' não encontrado. Iniciando processo de download e extração..."
# MAGIC
# MAGIC     wget -nc -P $VOLUME_PATH $URL_ORDER
# MAGIC     wget -nc -P $VOLUME_PATH $URL_CONSUMER
# MAGIC     wget -nc -P $VOLUME_PATH $URL_RESTAURANT
# MAGIC     wget -nc -P $VOLUME_PATH $URL_AB_TEST
# MAGIC
# MAGIC     tar -xvzf ${VOLUME_PATH}/ab_test_ref.tar.gz -C ${VOLUME_PATH}/ ab_test_ref.csv
# MAGIC     
# MAGIC     echo "Processo concluído."
# MAGIC else
# MAGIC     echo "Arquivo final '$FINAL_FILE_PATH' já existe. Nenhuma ação necessária."
# MAGIC fi
# MAGIC
# MAGIC echo "--- Conteúdo atual do Volume ---"
# MAGIC ls -l $VOLUME_PATH

# COMMAND ----------

# %sh
# # Analise estrutura do arquivo 

# zcat /Volumes/workspace/bronze_data/raw_files/order.json.gz | head -n 1 | python -m json.tool

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS workspace.bronze_data.orders

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ingestão Camada Bronze

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
    BooleanType, IntegerType, ArrayType, LongType
)

catalog = CATALOG_NAME
schema = SCHEMA_NAME
volume_path = VOLUME_PATH

print("Definindo os schemas explícitos para cada tabela...")

# Schema para orders
orders_schema = StructType([
    StructField("cpf", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("delivery_address_city", StringType(), True),
    StructField("delivery_address_country", StringType(), True),
    StructField("delivery_address_district", StringType(), True),
    StructField("delivery_address_external_id", StringType(), True),
    StructField("delivery_address_latitude", StringType(), True), 
    StructField("delivery_address_longitude", StringType(), True),
    StructField("delivery_address_state", StringType(), True),
    StructField("delivery_address_zip_code", StringType(), True),
    StructField("items", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_latitude", StringType(), True),
    StructField("merchant_longitude", StringType(), True),
    StructField("merchant_timezone", StringType(), True),
    StructField("order_created_at", TimestampType(), True),
    StructField("order_id", StringType(), True),
    StructField("order_scheduled", BooleanType(), True),
    StructField("order_total_amount", DoubleType(), True),
    StructField("origin_platform", StringType(), True),
    StructField("order_scheduled_date", TimestampType(), True)
])

# Schema para a tabela consumers
consumers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("language", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("active", BooleanType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_phone_area", StringType(), True),
    StructField("customer_phone_number", StringType(), True)
])

# Schema para a tabela restaurants
restaurants_schema = StructType([
    StructField("id", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("enabled", BooleanType(), True),
    StructField("price_range", IntegerType(), True),
    StructField("average_ticket", DoubleType(), True),
    StructField("delivery_time", DoubleType(), True),
    StructField("minimum_order_value", DoubleType(), True),
    StructField("merchant_zip_code", StringType(), True),
    StructField("merchant_city", StringType(), True),
    StructField("merchant_state", StringType(), True),
    StructField("merchant_country", StringType(), True)
])

# Schema para a tabela ab_test_users
ab_test_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("is_target", StringType(), True)
])


ISO_8601_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

common_csv_options = {
    "header": "true",
    "sep": ",",
    "timestampFormat": ISO_8601_TIMESTAMP_FORMAT
}

json_options = {
    "timestampFormat": ISO_8601_TIMESTAMP_FORMAT
}

files_to_process = {
    "orders":          ("json", f"{volume_path}/order.json.gz",      json_options,       orders_schema),
    "consumers":       ("csv",  f"{volume_path}/consumer.csv.gz",    common_csv_options, consumers_schema),
    "restaurants":     ("csv",  f"{volume_path}/restaurant.csv.gz",  common_csv_options, restaurants_schema),
    "ab_test_users":   ("csv",  f"{volume_path}/ab_test_ref.csv",    common_csv_options, ab_test_schema)
}


def ingest_data_with_schema(catalog, schema, table_name, file_format, file_path, read_options, table_schema):
    """
    Lê um arquivo de um volume usando um schema explícito e o salva como uma tabela
    gerenciada no Unity Catalog.
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"
    print(f"\nIniciando ingestão para a tabela {full_table_name}...")
    
    try:
        reader = spark.read.format(file_format).options(**read_options)
        
        
        if table_schema:
            reader = reader.schema(table_schema)
        else:
            
            raise ValueError("Schema explícito é obrigatório para ingestão.")

        df = reader.load(file_path)
        
        df.write.mode("overwrite").saveAsTable(full_table_name)
        print(f"Tabela {full_table_name} criada/atualizada com sucesso com schema validado.")
        
    except Exception as e:
        print(f"Erro ao processar a tabela {full_table_name}: {e}")



print("\nIniciando o pipeline de ingestão de dados da camada Bronze para a Prata...")
for table_name, (file_format, file_path, options, schema_def) in files_to_process.items():
    ingest_data_with_schema(catalog, schema, table_name, file_format, file_path, options, schema_def)

print("\n🚀 Processo de ingestão orientado por schema concluído!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Camada Silver - Validações 

# COMMAND ----------

from pyspark.sql.functions import col, count, when, row_number
from pyspark.sql.window import Window

catalog = "workspace"
bronze_schema = "bronze_data"
silver_schema = "silver_analytics"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

# Carregando as tabelas da camada Bronze
df_orders_bronze = spark.table(f"{catalog}.{bronze_schema}.orders")
df_consumers_bronze = spark.table(f"{catalog}.{bronze_schema}.consumers")
df_restaurants_bronze = spark.table(f"{catalog}.{bronze_schema}.restaurants")
df_ab_users_bronze = spark.table(f"{catalog}.{bronze_schema}.ab_test_users")


# Verificação de Qualidade dos Dados 

print("Iniciando verificações de qualidade dos dados...")

# --- Verificações de Chaves e Unicidade ---
print("\n--- Verificando chaves primárias e unicidade ---")

# Chaves primárias não podem ser nulas
assert df_orders_bronze.where(col("order_id").isNull()).count() == 0, "Verificação falhou: 'order_id' não pode ser nulo."
assert df_consumers_bronze.where(col("customer_id").isNull()).count() == 0, "Verificação falhou: 'customer_id' na tabela de consumidores não pode ser nulo."
assert df_ab_users_bronze.where(col("customer_id").isNull()).count() == 0, "Verificação falhou: 'customer_id' na tabela de teste A/B não pode ser nulo."
print("Verificação de chaves nulas ok.")

print("\n--- Correção de order_id ---")
duplicate_orders_df = df_orders_bronze.groupBy("order_id").count().filter("count > 1")
duplicate_count = duplicate_orders_df.count()

if duplicate_count > 0:
    print(f"Foram encontrados {duplicate_count} 'order_id's duplicados. Isso viola a unicidade da PK.")
    print("Investigando exemplos de duplicatas (ordenados pelo mais recente):")
    
    display(
        df_orders_bronze.join(duplicate_orders_df.select("order_id"), "order_id")
        .orderBy("order_id", col("order_created_at").desc())
        .limit(10)
    )
    

    
    print("\n Aplicando estratégia de deduplicação: mantendo o registro de pedido mais recente por 'order_id'.")
    window_spec = Window.partitionBy("order_id").orderBy(col("order_created_at").desc())
    
    df_orders_bronze = df_orders_bronze.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).drop("row_num")
    
    print(f"Deduplicação concluída.")
else:
    print(" Verificação de unicidade de 'order_id' passou sem encontrar duplicatas.")

# Verificação 2
assert df_orders_bronze.count() == df_orders_bronze.select("order_id").distinct().count(), "Verificação falhou: 'order_id' ainda não é único."
assert df_consumers_bronze.count() == df_consumers_bronze.select("customer_id").distinct().count(), "Verificação falhou: 'customer_id' na tabela de consumidores não é único."
print("Verificação de unicidade passou.")

print("\n--- Verificando a integridade do Teste A/B ---")

# Verificação 3
inconsistent_users = df_ab_users_bronze.groupBy("customer_id").count().filter("count > 1").count()
assert inconsistent_users == 0, f"Verificação falhou: Encontrados {inconsistent_users} clientes em mais de um grupo no teste A/B."
print("Verificação de consistência de grupos (nenhum usuário em múltiplos grupos) passou.")


# --- Verificação 4
print("\n--- Verificando integridade referencial e cobertura dos dados ---")

# Verificação 5: Todos os clientes que fizeram pedidos estão na tabela de consumidores?
orders_without_consumer = df_orders_bronze.select("customer_id").distinct().join(
    df_consumers_bronze,
    "customer_id",
    "left_anti"
).count()
if orders_without_consumer > 0:
    print(f"{orders_without_consumer} clientes que fizeram pedidos não foram encontrados na tabela de consumidores.")
else:
    print("Todos os clientes que fizeram pedidos existem na tabela de consumidores.")

# Verificação 6: Qual a porcentagem de clientes que fizeram pedidos que fazem parte do teste A/B?
ordering_customers_df = df_orders_bronze.select("customer_id").distinct()
total_ordering_customers = ordering_customers_df.count()

customers_not_in_test = ordering_customers_df.join(
    df_ab_users_bronze,
    "customer_id",
    "left_anti"
).count()

if total_ordering_customers > 0:
    coverage_percentage = ((total_ordering_customers - customers_not_in_test) / total_ordering_customers) * 100
    print(f"{coverage_percentage:.2f}% dos clientes que fizeram pedidos estão participando do teste A/B.")
    if customers_not_in_test > 0:
        print(f" -> {customers_not_in_test} clientes que fizeram pedidos não estão na base do teste.")
else:
    print("   -> Não há clientes com pedidos para calcular a cobertura do teste.")


print("\n--- Verificando a sanidade dos valores de negócio ---")

# Verificação 7
negative_orders = df_orders_bronze.where(col("order_total_amount") < 0).count()
assert negative_orders == 0, f"Verificação falhou: Encontrados {negative_orders} pedidos com valor total negativo."
print("Verificação de valores de pedido não-negativos passou.")

print("\n Verificações de qualidade concluídas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tratamento Tabela Orders

# COMMAND ----------

import numpy as np
from pyspark.sql.functions import (
    col, from_json, transform, aggregate, when, size, 
    lit, exists, array_max, flatten, struct, coalesce, array, filter as spark_filter, concat,
    udf
)
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DoubleType

# UDF para Desvio Padrão ---

def calculate_stddev_udf(price_list: list) -> float:
    """Calcula o desvio padrão de uma lista; retorna 0.0 para listas com menos de 2 elementos."""
    if price_list is None or len(price_list) < 2:
        return 0.0
    return float(np.std(price_list))

spark_stddev_udf = udf(calculate_stddev_udf, DoubleType())


# Schema para a coluna 'items'

items_schema = ArrayType(StructType([
    StructField("quantity", DoubleType(), True),
    StructField("unitPrice", StructType([StructField("value", StringType(), True)]), True),
    StructField("totalValue", StructType([StructField("value", StringType(), True)]), True),
    StructField("totalAddition", StructType([StructField("value", StringType(), True)]), True),
    StructField("totalDiscount", StructType([StructField("value", StringType(), True)]), True),
    StructField("garnishItems", ArrayType(StructType([
        StructField("unitPrice", StructType([StructField("value", StringType(), True)]), True),
        StructField("totalValue", StructType([StructField("value", StringType(), True)]), True)
    ])), True)
]))



df_parsed = df_orders_bronze \
    .withColumn("items_parsed", from_json(col("items"), items_schema))

df_components_consolidated = df_parsed \
    .withColumn("all_price_components", flatten(transform(col("items_parsed"),lambda item: concat(array(struct(item.unitPrice.alias("unitPrice"),item.totalValue.alias("totalValue"))),transform(coalesce(item.garnishItems, array()),lambda garnish: struct(garnish.unitPrice.alias("unitPrice"),garnish.totalValue.alias("totalValue"))))))) \
    .withColumn("lista_precos_unitarios", transform(spark_filter(col("all_price_components"), lambda comp: comp.unitPrice.value.cast(DoubleType()) > 0), lambda comp: (comp.unitPrice.value.cast(DoubleType()) / 100.0)))


# Engenharia de Features

df_features = df_components_consolidated \
    .withColumn("quantidade_total_de_itens", aggregate("items_parsed", lit(0.0), lambda acc, item: acc + item.quantity)) \
    .withColumn("preco_medio_por_item", when(size("lista_precos_unitarios") > 0, aggregate("lista_precos_unitarios", lit(0.0), lambda acc, x: acc + x) / size("lista_precos_unitarios")).otherwise(0.0)) \
    .withColumn("item_mais_caro_no_carrinho", array_max("lista_precos_unitarios")) \
    .withColumn("desvio_padrao_preco_itens", spark_stddev_udf(col("lista_precos_unitarios"))) \
    .withColumn("valor_total_do_pedido_recalculado", aggregate("all_price_components", lit(0.0), lambda acc, comp: acc + (comp.totalValue.value.cast(DoubleType()) / 100.0))) \
    .withColumn("total_de_adicionais_pedido", aggregate("items_parsed", lit(0.0), lambda acc, item: acc + (item.totalAddition.value.cast(DoubleType()) / 100.0))) \
    .withColumn("pedido_teve_desconto_restaurante", exists("items_parsed", lambda item: item.totalDiscount.value.cast(DoubleType()) > 0))


# Feature Percentual

df_final_features = df_features \
    .withColumn("percentual_valor_item_mais_caro", when((col("valor_total_do_pedido_recalculado") > 0) & (col("item_mais_caro_no_carrinho").isNotNull()), (col("item_mais_caro_no_carrinho") / col("valor_total_do_pedido_recalculado")) * 100).otherwise(0.0))

orders_cleaned = df_final_features.select(
    "order_id",
    "customer_id",
    "merchant_id",
    "order_created_at",
    "order_total_amount",
    "origin_platform",
    col("quantidade_total_de_itens").cast("int"),
    "preco_medio_por_item",
    "item_mais_caro_no_carrinho",
    "desvio_padrao_preco_itens",
    "percentual_valor_item_mais_caro",
    "total_de_adicionais_pedido",
    "pedido_teve_desconto_restaurante",
    "valor_total_do_pedido_recalculado"
)

print("Tabela 'orders' limpa e enriquecida com features robustas.")
print("Amostra do resultado:")
display(orders_cleaned)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Seleção de variaveis relevantes

# COMMAND ----------

from pyspark.sql.functions import col, when, hour


print("Selecinando as colunas de interesse de cada tabela...")

orders_cleaned.createOrReplaceTempView("orders_cleaned_view")
orders_selected = spark.sql("SELECT * FROM orders_cleaned_view")

consumers_selected = spark.sql(f"""
    SELECT
        customer_id,
        created_at as customer_creation_date,
        active AS is_customer_active
    FROM {catalog}.{bronze_schema}.consumers
""")

restaurants_selected = spark.sql(f"""
    SELECT
        id AS restaurant_id,
        enabled AS is_restaurant_active,
        price_range AS restaurant_price_range,
        created_at AS restaurant_creation_date,
        average_ticket AS restaurant_average_ticket,
        delivery_time AS restaurant_delivery_time,
        minimum_order_value AS restaurant_minimum_order_value,
        merchant_state AS restaurant_state,
        merchant_city AS restaurant_city_id
    FROM {catalog}.{bronze_schema}.restaurants
""")

ab_users_selected = spark.sql(f"""
    SELECT
        customer_id,
        is_target as ab_group
    FROM {catalog}.{bronze_schema}.ab_test_users
""")

print(" Componentes selecionados e prontos para a consolidação.")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Consolidação Tabela ABT 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, when, hour, datediff, lag, avg as spark_avg, row_number
from pyspark.sql.window import Window


# Consolidação da Tabela Analítica (Joins)

print("Consolidando todos os componentes com Left Joins")

abt_consolidated = orders_cleaned.alias("o") \
    .join(consumers_selected.alias("c"), col("o.customer_id") == col("c.customer_id"), how="left") \
    .join(restaurants_selected.alias("r"), col("o.merchant_id") == col("r.restaurant_id"), how="left") \
    .join(ab_users_selected.alias("ab"), col("o.customer_id") == col("ab.customer_id"), how="left")

print("Consolidação concluída.")

print("Enriquecendo a tabela com features de contexto e histórico do cliente...")

#Janelas de Análise 
customer_window = Window.partitionBy(col("o.customer_id")).orderBy(col("o.order_created_at"))
customer_restaurant_window = Window.partitionBy(col("o.customer_id"), col("r.restaurant_id")).orderBy(col("o.order_created_at"))

# Transformações
abt_enriched = abt_consolidated \
    .withColumn(
        "dias_cliente_ativo_no_pedido",
        
        datediff(col("o.order_created_at"), col("c.customer_creation_date"))
    ) \
    .withColumn(
        "relacao_pedido_ticket_medio",
        
        when(col("r.restaurant_average_ticket") > 0,
             col("o.order_total_amount") / col("r.restaurant_average_ticket")
        ).otherwise(None)
    ) \
    .withColumn(
        "periodo_do_dia",
       
        when(hour(col("o.order_created_at")).between(6, 11), "Manhã")
        .when(hour(col("o.order_created_at")).between(12, 17), "Tarde")
        .when(hour(col("o.order_created_at")).between(18, 23), "Noite")
        .otherwise("Madrugada")
    ) \
    .withColumn(
        "previous_order_date",
        
        lag(col("o.order_created_at"), 1).over(customer_window)
    ) \
    .withColumn(
        "gasto_medio_historico_cliente",
        
        spark_avg(col("o.order_total_amount")).over(
            customer_window.rowsBetween(Window.unboundedPreceding, -1)
        )
    ) \
    .withColumn(
        "frequencia_cliente_no_restaurante",
        
        row_number().over(customer_restaurant_window)
    ) \
    .withColumn(
        "dias_desde_ultimo_pedido",
        
        datediff(col("o.order_created_at"), col("previous_order_date"))
    )

print("Enriquecimento completo finalizado.")


abt_final_table_name = f"{catalog}.{silver_schema}.orders_abt_final"
print(f"Selecionando e filtrando as colunas finais para salvar em '{abt_final_table_name}'...")

abt_final = abt_enriched \
    .select(
        
        col("o.order_id"), col("o.customer_id"), col("r.restaurant_id"),
        col("ab.ab_group"), 
        
        
        col("o.order_created_at"), "periodo_do_dia",
        
       
        "order_total_amount", "quantidade_total_de_itens", "preco_medio_por_item",
        "item_mais_caro_no_carrinho", "desvio_padrao_preco_itens",
        "percentual_valor_item_mais_caro", "pedido_teve_desconto_restaurante",
        
        
        "dias_desde_ultimo_pedido",
        "gasto_medio_historico_cliente",
        "frequencia_cliente_no_restaurante",
        "dias_cliente_ativo_no_pedido",
        "relacao_pedido_ticket_medio",
        
       
        "origin_platform", 
        col("c.is_customer_active"),
        col("r.restaurant_price_range"),
        col("r.restaurant_delivery_time")
    )


abt_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(abt_final_table_name)

print("\nTabela Analítica Base (ABT) COMPLETA foi criada com sucesso!")
display(spark.table(abt_final_table_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Verificações

# COMMAND ----------

from pyspark.sql.functions import col

ab_users_table = f"{catalog}.bronze_data.ab_test_users" 
ab_users_df = spark.table(ab_users_table).filter(col("is_target").isin("target", "control")).select("customer_id").distinct()
orders_df = abt_final.select("customer_id").distinct()

matched_users = ab_users_df.join(orders_df, "customer_id", "inner").count()
total_ab_users = ab_users_df.count()
missing_users = total_ab_users - matched_users

print(f"Total de usuários em ab_users_table: {total_ab_users}")
print(f"Usuários com pedidos em abt_table: {matched_users}")
print(f"Usuários sem pedidos: {missing_users}")
print(f"Percentual de usuários com pedidos: {(matched_users / total_ab_users) * 100:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Camada Gold
# MAGIC #####Validações

# COMMAND ----------

from pyspark.sql.functions import col, count, countDistinct

catalog = "workspace"
gold_schema = "gold_analytics"
abt_table = f"{catalog}.silver_analytics.orders_abt_final"
ab_users_table = f"{catalog}.bronze_data.ab_test_users"
customers_table = f"{catalog}.bronze_data.consumers"

print("\nValidações de integridade dos dados...")

ab_users_df = spark.table(ab_users_table)\
    .filter(col("is_target").isin("target", "control"))\
    .select("customer_id", col("is_target").alias("ab_group"))\
    .distinct()
total_ab_users = ab_users_df.count()
print(f"1. Total de usuários em ab_users_table: {total_ab_users}")


group_counts = ab_users_df.groupBy("ab_group").agg(count("customer_id").alias("total_usuarios"))
group_counts.show()



orders_df = spark.table(abt_table).select("customer_id").distinct()
matched_users = ab_users_df.join(orders_df, "customer_id", "inner").count()
missing_users = total_ab_users - matched_users
print(f"2. Usuários com pedidos em abt_table: {matched_users}")
print(f"3. Usuários sem pedidos: {missing_users}")
print(f"4. Percentual de usuários com pedidos: {(matched_users / total_ab_users) * 100:.2f}%")


customers_df = spark.table(customers_table).select("customer_id").distinct()
customers_in_ab = customers_df.join(ab_users_df, "customer_id", "inner").count()
customers_not_in_ab = customers_df.count() - customers_in_ab
print(f"5. Usuários de customers em ab_users_table: {customers_in_ab}")
print(f"6. Usuários de customers fora de ab_users_table: {customers_not_in_ab}")



orders_not_in_customers = orders_df.join(customers_df, "customer_id", "left_anti").count()
print(f"7. Clientes com pedidos não em customers: {orders_not_in_customers}")



orders_not_in_ab = orders_df.join(ab_users_df, "customer_id", "left_anti").count()
print(f"8. Clientes com pedidos não em ab_users_table: {orders_not_in_ab}")



duplicates_ab_users = ab_users_df.groupBy("customer_id").agg(count("*").alias("count")).filter(col("count") > 1).count()
print(f"9. Usuários duplicados em ab_users_table: {duplicates_ab_users}")



orders_period = spark.sql(f"""
    SELECT MIN(order_created_at) as min_date, MAX(order_created_at) as max_date
    FROM {abt_table}
""").show()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Cálculo de Métricas

# COMMAND ----------

# Métricas de negócio por usuário
from pyspark.sql.functions import sum, count, avg, col

print("\nCalculando métricas de negócio por usuário...")


user_level_kpis_df = spark.sql(f"""
    SELECT
        customer_id,
        SUM(order_total_amount) as receita_total_usuario,
        COUNT(order_id) as frequencia_usuario,
        AVG(order_total_amount) as ticket_medio_usuario, 
        AVG(quantidade_total_de_itens) as media_itens_usuario,
        AVG(dias_desde_ultimo_pedido) as media_dias_recompra_usuario,
        SUM(CASE WHEN frequencia_cliente_no_restaurante = 1 THEN 1 ELSE 0 END) as total_pedidos_novos_restaurantes
    FROM {abt_table}
    GROUP BY customer_id
""").withColumn(
    "taxa_pedidos_novos_restaurantes_usuario", 
    (col("total_pedidos_novos_restaurantes") / col("frequencia_usuario")) * 100
)

print(f"Total de usuários com pedidos: {user_level_kpis_df.count()}")
user_level_kpis_df.display()




# COMMAND ----------

from pyspark.sql.functions import datediff, when, col
from pyspark.sql.window import Window


print("\nCalculando flags de retenção 7d e 14d...")


retention_flags_df = spark.sql(f"""
    WITH ranked_orders AS (
        SELECT
            customer_id,
            order_created_at,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_created_at) as order_rank
        FROM {abt_table}
    )
    SELECT
        customer_id,
        MAX(CASE WHEN order_rank = 1 THEN order_created_at END) AS first_order_date,
        MAX(CASE WHEN order_rank = 2 THEN order_created_at END) AS second_order_date
    FROM ranked_orders
    GROUP BY customer_id
""").withColumn("is_retained_7d", when(datediff(col("second_order_date"), col("first_order_date")) <= 7, 1).otherwise(0))\
    .withColumn("is_retained_14d", when(datediff(col("second_order_date"), col("first_order_date")) <= 14, 1).otherwise(0))


print(f"Total de usuários com retenção calculada: {retention_flags_df.count()}")
retention_flags_df.display()



# COMMAND ----------

# Agregando KPIs e criando tabela Gold
from pyspark.sql.functions import avg, stddev, sum, count, col, when

print("\nAgregando KPIs e criando tabela Gold...")

all_users_with_metrics_df = ab_users_df.join(
    user_level_kpis_df, "customer_id", "left"
).join(
    retention_flags_df.select("customer_id", "is_retained_7d", "is_retained_14d"), "customer_id", "left"
).na.fill({
    "frequencia_usuario": 0,
    "receita_total_usuario": 0,
    "ticket_medio_usuario": 0,
    "media_itens_usuario": 0,
    "media_dias_recompra_usuario": 0,
    "taxa_pedidos_novos_restaurantes_usuario": 0,
    "is_retained_7d": 0,
    "is_retained_14d": 0
})


df_kpi_summary = all_users_with_metrics_df.groupBy("ab_group").agg(
   
    avg("receita_total_usuario").alias("receita_por_cliente"),
    stddev("receita_total_usuario").alias("stddev_receita_por_cliente"),
    avg("frequencia_usuario").alias("frequencia_media_pedidos"),
    stddev("frequencia_usuario").alias("stddev_frequencia_media_pedidos"),
    
    avg("ticket_medio_usuario").alias("ticket_medio"),
    stddev("ticket_medio_usuario").alias("stddev_ticket_medio"),

   
    (avg("is_retained_7d") * 100).alias("taxa_retencao_7d_pct"),
    (stddev("is_retained_7d") * 100).alias("stddev_retencao_7d"),
    (avg("is_retained_14d") * 100).alias("taxa_retencao_14d_pct"),
    (stddev("is_retained_14d") * 100).alias("stddev_retencao_14d"),
    
    
    avg("media_itens_usuario").alias("itens_por_pedido"),
    stddev("media_itens_usuario").alias("stddev_itens_por_pedido"),
    avg("media_dias_recompra_usuario").alias("tempo_medio_para_proxima_compra"),
    stddev("media_dias_recompra_usuario").alias("stddev_tempo_medio_para_proxima_compra"),
    # CORRIGIDO: Agora calculamos a média e o stddev da taxa por usuário
    avg("taxa_pedidos_novos_restaurantes_usuario").alias("taxa_pedidos_novos_restaurantes_pct"),
    stddev("taxa_pedidos_novos_restaurantes_usuario").alias("stddev_taxa_pedidos_novos_restaurantes_pct"),
    
    
    count("customer_id").alias("total_usuarios"),
    count(when(col("frequencia_usuario") > 0, True)).alias("usuarios_convertidos")
)

# Salvar
gold_summary_table_name = f"{catalog}.{gold_schema}.abtest_kpi_summary_retention"

df_kpi_summary.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(gold_summary_table_name)
print(f"Tabela Gold salva: {gold_summary_table_name}")
df_kpi_summary.display()


# COMMAND ----------


#Testes de Significância
import pandas as pd
from scipy.stats import ttest_ind_from_stats

pd_kpi_summary = df_kpi_summary.toPandas()
pd_kpi_summary = pd_kpi_summary.rename(columns={"ab_group": "group_name"})

def run_t_test(metric_mean, metric_stddev, metric_n, metric_label):

    control = pd_kpi_summary[pd_kpi_summary["group_name"] == "control"].iloc[0]
    target = pd_kpi_summary[pd_kpi_summary["group_name"] == "target"].iloc[0]
    mean_control = control[metric_mean]; mean_target = target[metric_mean]
    std_control = control[metric_stddev]; std_target = target[metric_stddev]
    n_control = control[metric_n]; n_target = target[metric_n]
    if pd.isna(std_control) or pd.isna(std_target) or std_control == 0 or std_target == 0 or n_control <= 1 or n_target <= 1:
        print(f"\n--- {metric_label} ---\n    Teste T não calculado: desvio padrão ou N inválido.")
        return
    stat, p_value = ttest_ind_from_stats(mean1=mean_target, std1=std_target, nobs1=n_target, mean2=mean_control, std2=std_control, nobs2=n_control, alternative="two-sided")
    print(f"\n--- {metric_label} ---")
    print(f"Média Controle: {mean_control:.2f} | Média Target: {mean_target:.2f}")
    lift = ((mean_target - mean_control) / mean_control) * 100 if mean_control != 0 else float("inf")
    print(f"Lift: {lift:.2f}%")
    print(f"P-valor: {p_value:.4f}")
    print("Resultado: " + ("Significativa" if p_value < 0.05 else "Não significativa"))


print("\nExecutando testes estatísticos com consistência...")


print("\n--- GRUPO 1: MÉTRICAS GERAIS POR USUÁRIO")
run_t_test('receita_por_cliente', 'stddev_receita_por_cliente', 'total_usuarios', 'Receita por Cliente (ARPU)')
run_t_test('frequencia_media_pedidos', 'stddev_frequencia_media_pedidos', 'total_usuarios', 'Frequência Média de Pedidos')
run_t_test('taxa_retencao_7d_pct', 'stddev_retencao_7d', 'total_usuarios', 'Taxa de Retenção 7 Dias')
run_t_test('taxa_retencao_14d_pct', 'stddev_retencao_14d', 'total_usuarios', 'Taxa de Retenção 14 Dias')


print("\n--- GRUPO 2: MÉTRICAS DE COMPORTAMENTO")
run_t_test('ticket_medio', 'stddev_ticket_medio', 'usuarios_convertidos', 'Ticket Médio (por usuário)')
run_t_test('itens_por_pedido', 'stddev_itens_por_pedido', 'usuarios_convertidos', 'Média de Itens por Pedido (por usuário)')
run_t_test('tempo_medio_para_proxima_compra', 'stddev_tempo_medio_para_proxima_compra', 'usuarios_convertidos', 'Tempo Médio para Próxima Compra')
run_t_test('taxa_pedidos_novos_restaurantes_pct', 'stddev_taxa_pedidos_novos_restaurantes_pct', 'usuarios_convertidos', 'Taxa de Pedidos em Novos Restaurantes (por usuário)')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise com Segmentação 

# COMMAND ----------

from pyspark.sql.functions import col, min, max, count, when, datediff

print("\nSegmentando usuários por ciclo de vida...")

# data de referência (final do período do teste A/B)
reference_date = '2019-01-31'

segmented_users_df = spark.sql(f"""
    WITH user_activity AS (
        SELECT
            customer_id,
            MIN(order_created_at) as first_order_date,
            MAX(order_created_at) as last_order_date,
            COUNT(order_id) as total_orders,
            SUM(CASE WHEN datediff('{reference_date}', order_created_at) <= 30 THEN 1 ELSE 0 END) as orders_last_30_days
        FROM {abt_table}
        GROUP BY customer_id
    )
    SELECT
        customer_id,
        CASE
            WHEN first_order_date >= date_sub('{reference_date}', 7) THEN 'new'
            WHEN orders_last_30_days >= 2 THEN 'active'
            WHEN datediff('{reference_date}', last_order_date) BETWEEN 31 AND 90 THEN 'at_risk'
            ELSE 'other'
        END as lifecycle_segment
    FROM user_activity
""")


segmented_users_df = segmented_users_df.join(ab_users_df.select("customer_id", "ab_group"), "customer_id", "inner")


segmented_metrics_df = segmented_users_df.join(
    user_level_kpis_df, "customer_id", "left"
).join(
    retention_flags_df.select("customer_id", "is_retained_7d", "is_retained_14d"), "customer_id", "left"
).na.fill({
    "frequencia_usuario": 0,
    "receita_total_usuario": 0,
    "ticket_medio_usuario": 0,
    "media_itens_usuario": 0,
    "media_dias_recompra_usuario": 0,
    "taxa_pedidos_novos_restaurantes_usuario": 0,
    "is_retained_7d": 0,
    "is_retained_14d": 0
})


df_segmented_kpi_summary = segmented_metrics_df.groupBy("lifecycle_segment", "ab_group").agg(
    avg("receita_total_usuario").alias("receita_por_cliente"),
    stddev("receita_total_usuario").alias("stddev_receita_por_cliente"),
    avg("frequencia_usuario").alias("frequencia_media_pedidos"),
    stddev("frequencia_usuario").alias("stddev_frequencia_media_pedidos"),
    avg("ticket_medio_usuario").alias("ticket_medio"),
    stddev("ticket_medio_usuario").alias("stddev_ticket_medio"),
    (avg("is_retained_14d") * 100).alias("taxa_retencao_14d_pct"),
    (stddev("is_retained_14d") * 100).alias("stddev_retencao_14d"),
    avg("media_dias_recompra_usuario").alias("tempo_medio_para_proxima_compra"),
    stddev("media_dias_recompra_usuario").alias("stddev_tempo_medio_para_proxima_compra"),
    avg("taxa_pedidos_novos_restaurantes_usuario").alias("taxa_pedidos_novos_restaurantes_pct"),
    stddev("taxa_pedidos_novos_restaurantes_usuario").alias("stddev_taxa_pedidos_novos_restaurantes_pct"),
    count("customer_id").alias("total_usuarios"),
    count(when(col("frequencia_usuario") > 0, True)).alias("usuarios_convertidos")
)


segmented_summary_table_name = f"{catalog}.{gold_schema}.abtest_kpi_summary_segmented"
df_segmented_kpi_summary.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(segmented_summary_table_name)
print(f"Tabela segmentada salva: {segmented_summary_table_name}")
df_segmented_kpi_summary.display()


# COMMAND ----------

import pandas as pd
from scipy.stats import ttest_ind_from_stats

print("\nExecutando testes estatísticos por segmento...")


pd_segmented_kpi_summary = df_segmented_kpi_summary.toPandas()
pd_segmented_kpi_summary = pd_segmented_kpi_summary.rename(columns={"ab_group": "group_name"})

def run_segmented_t_test(segment, metric_mean, metric_stddev, metric_n, metric_label):
    df_segment = pd_segmented_kpi_summary[pd_segmented_kpi_summary["lifecycle_segment"] == segment]
    if len(df_segment) < 2:
        print(f"\n--- {metric_label} ({segment}) ---\n    Teste T não calculado")
        return
    control = df_segment[df_segment["group_name"] == "control"].iloc[0]
    target = df_segment[df_segment["group_name"] == "target"].iloc[0]
    mean_control = control[metric_mean]; mean_target = target[metric_mean]
    std_control = control[metric_stddev]; std_target = target[metric_stddev]
    n_control = control[metric_n]; n_target = control[metric_n]
    if pd.isna(std_control) or pd.isna(std_target) or std_control == 0 or std_target == 0 or n_control <= 1 or n_target <= 1:
        print(f"\n--- {metric_label} ({segment}) ---\n    Teste T não calculado")
        return
    stat, p_value = ttest_ind_from_stats(mean1=mean_target, std1=std_target, nobs1=n_target, mean2=mean_control, std2=std_control, nobs2=n_control, alternative="two-sided")
    print(f"\n--- {metric_label} ({segment}) ---")
    print(f"Média Controle: {mean_control:.2f} | Média Target: {mean_target:.2f}")
    lift = ((mean_target - mean_control) / mean_control) * 100 if mean_control != 0 else float("inf")
    print(f"Lift: {lift:.2f}%")
    print(f"P-valor: {p_value:.4f}")
    print("Resultado: " + ("Significativa" if p_value < 0.05 else "Não significativa"))

# Testes por segmento
segments = ["new", "active", "at_risk"]
metrics = [
    ("receita_por_cliente", "stddev_receita_por_cliente", "total_usuarios", "Receita por Cliente (ARPU)"),
    ("frequencia_media_pedidos", "stddev_frequencia_media_pedidos", "total_usuarios", "Frequência Média de Pedidos"),
    ("taxa_retencao_14d_pct", "stddev_retencao_14d", "total_usuarios", "Taxa de Retenção 14 Dias"),
    ("tempo_medio_para_proxima_compra", "stddev_tempo_medio_para_proxima_compra", "usuarios_convertidos", "Tempo Médio para Próxima Compra"),
    ("taxa_pedidos_novos_restaurantes_pct", "stddev_taxa_pedidos_novos_restaurantes_pct", "usuarios_convertidos", "Taxa de Pedidos em Novos Restaurantes")
]

for segment in segments:
    print(f"\nTestes para o segmento: {segment}")
    for metric_mean, metric_stddev, metric_n, metric_label in metrics:
        run_segmented_t_test(segment, metric_mean, metric_stddev, metric_n, metric_label)
