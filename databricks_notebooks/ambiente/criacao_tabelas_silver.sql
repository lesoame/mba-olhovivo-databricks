-- Criação das tabelas silver apontando para o storage da azure

-- dim_empresas
CREATE TABLE IF NOT EXISTS olhovivo_silver.dim_empresas
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/silver/dim_empresas';

-- dim_linhas
CREATE TABLE IF NOT EXISTS olhovivo_silver.dim_linhas
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/silver/dim_linhas';

-- fato_posicao_veiculos
CREATE TABLE IF NOT EXISTS olhovivo_silver.fato_posicao_veiculos
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/silver/fato_posicao_veiculos';

-- mapa_shapes_gtfs
CREATE TABLE IF NOT EXISTS olhovivo_silver.mapa_shapes_gtfs
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/silver/mapa_shapes_gtfs';

