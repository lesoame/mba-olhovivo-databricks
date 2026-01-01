# Databricks notebook source
# ============================================================
# Camada Silver: Dimensão Empresas
# Fonte: API Olho Vivo (/Empresa)
# ============================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

df_empresas = spark.table("olhovivo_bronze.empresas")

# Deduplicação (pegar último status da empresa)
w_emp = Window.partitionBy("c").orderBy(col("dt_ingestao").desc())

df_silver = df_empresas.withColumn("rn", row_number().over(w_emp)) \
    .filter(col("rn") == 1) \
    .select(
        col("c").cast("int").alias("cod_empresa"),
        col("n").alias("nome_empresa"), # Ex: GATO PRETO
        col("a").cast("int").alias("cod_area_operacao")
    )

df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("olhovivo_silver.dim_empresas")
print("✅ Dimensão Empresas criada!")
display(df_silver)