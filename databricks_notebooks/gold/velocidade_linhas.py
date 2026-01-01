# Databricks notebook source
# ============================================================
# Camada Gold: Cálculo de Velocidade Média (Engenharia Pura!)
# ============================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, unix_timestamp, round, radians, sin, cos, atan2, sqrt, avg, lit
from pyspark.sql.window import Window

# 1. Ler Fato Prata
df_posicao = spark.table("olhovivo_silver.fato_posicao_veiculos")

# 2. Janela para olhar o "ponto anterior" do MESMO veículo
w_calc = Window.partitionBy("prefixo_veiculo").orderBy("data_hora_sp")

# 3. Engenharia de Features: Calcular Distância e Tempo entre pings
# Usamos a fórmula de Haversine simplificada para distância em KM
df_calc = df_posicao.withColumn("lat_prev", lag("latitude").over(w_calc)) \
                    .withColumn("lon_prev", lag("longitude").over(w_calc)) \
                    .withColumn("time_prev", lag("data_hora_sp").over(w_calc))

# Filtra apenas onde temos dados anteriores (descarta o primeiro registro)
df_calc = df_calc.filter(col("lat_prev").isNotNull())

# 4. Cálculo Matemático (Distância em KM)
# R = Raio da Terra (6371 km)
# (Cálculo aproximado via SQL functions)
df_velocidade = df_calc.withColumn("dist_km", 
    round(
        lit(6371) * 2 * atan2(
            sqrt(
                (sin(radians(col("latitude") - col("lat_prev")) / 2) ** 2) +
                cos(radians(col("lat_prev"))) * cos(radians(col("latitude"))) * (sin(radians(col("longitude") - col("lon_prev")) / 2) ** 2)
            ),
            sqrt(1 - (
                (sin(radians(col("latitude") - col("lat_prev")) / 2) ** 2) +
                cos(radians(col("lat_prev"))) * cos(radians(col("latitude"))) * (sin(radians(col("longitude") - col("lon_prev")) / 2) ** 2)
            ))
        )
    , 4)
).withColumn("tempo_horas", 
    (unix_timestamp(col("data_hora_sp")) - unix_timestamp(col("time_prev"))) / 3600
)

# 5. Velocidade (Km/h) = Distância / Tempo
df_final = df_velocidade.withColumn("velocidade_kmh", round(col("dist_km") / col("tempo_horas"), 1))

# Filtrar ruídos (GPS pulando, velocidade > 120km/h ou paradas muito longas)
# df_clean = df_final.filter((col("velocidade_kmh") > 0) & (col("velocidade_kmh") < 100))
df_clean = df_final.filter((col("velocidade_kmh") > 5) & (col("velocidade_kmh") < 110))

# 6. Agregação por Linha (KPI Final)
df_kpi_velocidade = df_clean.groupBy("cod_linha", "sentido_desc", "data_referencia").agg(
    avg("velocidade_kmh").alias("velocidade_media"),
    avg("latitude").alias("lat_centroide"), # Para plotar no mapa onde é o gargalo
    avg("longitude").alias("lon_centroide")
)

# Join para pegar o nome da linha
df_linhas = spark.table("olhovivo_silver.dim_linhas")
df_gold_velocidade = df_kpi_velocidade.join(df_linhas, "cod_linha", "left")

# Gravar Gold
df_gold_velocidade.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("olhovivo_gold.velocidade_linhas") 

print("✅ KPI de Velocidade calculado! Agora você sabe quais linhas voam e quais rastejam.")
display(df_gold_velocidade.orderBy(col("velocidade_media").desc()).limit(10))