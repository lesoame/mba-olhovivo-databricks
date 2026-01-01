# Databricks notebook source
# ============================================================
# Camada Silver: Fato Posição (Clean & Optimized)
# ============================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, from_utc_timestamp, to_date, row_number, when
from pyspark.sql.window import Window

df_bronze = spark.table("olhovivo_bronze.posicao_veiculos")

# Deduplicação: Evitar que o mesmo sinal de GPS apareça duplicado
# Chave: Veículo + Hora do Sinal (ta)
w_dedup = Window.partitionBy("p", "ta").orderBy("dt_ingestao")

df_silver = df_bronze.withColumn("rn", row_number().over(w_dedup)) \
    .filter(col("rn") == 1) \
    .select(
        # Ajuste de Fuso (UTC -> SP)
        from_utc_timestamp(to_timestamp(col("ta")), "America/Sao_Paulo").alias("data_hora_sp"),
        
        col("p").cast("int").alias("prefixo_veiculo"),
        col("cl").cast("int").alias("cod_linha"), # Chave para dim_linhas
        
        # Tratamento de coordenadas
        col("py").alias("latitude"),
        col("px").alias("longitude"),
        
        # Descrição de sentido
        when(col("sl") == 1, "Ida").otherwise("Volta").alias("sentido_desc"),
        col("a").alias("is_acessivel")
    )

# Cria coluna de data para particionamento físico (Performance Power BI)
df_silver = df_silver.withColumn("data_referencia", to_date(col("data_hora_sp")))

# Gravação Particionada
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("data_referencia") \
    .saveAsTable("olhovivo_silver.fato_posicao_veiculos")

print("✅ Fato Posição atualizada!")
display(df_silver.limit(5))