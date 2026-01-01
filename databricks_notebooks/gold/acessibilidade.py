# Databricks notebook source
# ============================================================
# Camada Gold: KPI de Acessibilidade
# ============================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, when, round

df_posicao = spark.table("olhovivo_silver.fato_posicao_veiculos")
df_linhas = spark.table("olhovivo_silver.dim_linhas")

# Agrupa por Linha e Data
df_acessibilidade = df_posicao.groupBy("cod_linha", "data_referencia").agg(
    count("prefixo_veiculo").alias("total_veiculos_reportados"),
    # Soma 1 se acessivel for true
    sum(when(col("is_acessivel") == True, 1).otherwise(0)).alias("total_acessiveis")
)

# Calcula Porcentagem
df_kpi = df_acessibilidade.withColumn("percentual_acessibilidade", 
    round((col("total_acessiveis") / col("total_veiculos_reportados")) * 100, 2)
)

# Join com nomes
df_gold = df_kpi.join(df_linhas, "cod_linha", "left").select(
    "nome_api_tp_ts",
    "letreiro_completo",
    "percentual_acessibilidade",
    "total_veiculos_reportados",
    "data_referencia"
)

# Gravar
df_gold.write.format("delta").mode("overwrite").saveAsTable("olhovivo_gold.acessibilidade")

print("✅ Ranking de Acessibilidade criado!")
display(df_gold.orderBy(col("percentual_acessibilidade").asc()).limit(10))