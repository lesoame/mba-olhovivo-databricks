-- CALCULA A DISTÂNCIA TOTAL DE CADA SHAPE 

CREATE OR REPLACE TEMPORARY VIEW vw_shape_geometria AS
WITH shape_lag AS (
    SELECT 
        shape_id,
        shape_pt_sequence,
        shape_pt_lat as lat1,
        shape_pt_lon as lon1,
        LEAD(shape_pt_lat) OVER (PARTITION BY shape_id ORDER BY shape_pt_sequence) as lat2,
        LEAD(shape_pt_lon) OVER (PARTITION BY shape_id ORDER BY shape_pt_sequence) as lon2
    FROM olhovivo_bronze.gtfs_shapes
)
SELECT 
    shape_id,
    -- Soma das distâncias entre os pontos = Extensão total em KM
    CAST(SUM(
        2 * 6371 * ASIN(
            SQRT(
                POWER(SIN((RADIANS(lat2) - RADIANS(lat1)) / 2), 2) +
                COS(RADIANS(lat1)) * COS(RADIANS(lat2)) *
                POWER(SIN((RADIANS(lon2) - RADIANS(lon1)) / 2), 2)
            )
        )
    ) AS DECIMAL(10,2)) as extensao_km
FROM shape_lag
WHERE lat2 IS NOT NULL
GROUP BY shape_id;

-- CRIA A TABELA GOLD UNIFICADA (Rota -> Trip -> Shape -> Distância)

CREATE OR REPLACE TABLE olhovivo_gold.perfil_rota_estatico AS
SELECT 
    t.route_id as letreiro_gtfs, 
    
    -- Como uma linha pode ter variações de trajeto, pegamos a extensão MÉDIA ou MÁXIMA
    -- AVG ajuda a suavizar pequenas diferenças de shapes
    AVG(s.extensao_km) as extensao_km,
    
    -- Estimativa de paradas (se não tivermos stops vinculados ainda, usamos proxy por km)
    -- SPTrans média é ~3 paradas por km em área urbana
    CAST(AVG(s.extensao_km) * 3 AS INT) as qtd_paradas_estimada
FROM 
    olhovivo_bronze.gtfs_trips t
JOIN 
    vw_shape_geometria s ON t.shape_id = s.shape_id
GROUP BY 
    t.route_id;    
