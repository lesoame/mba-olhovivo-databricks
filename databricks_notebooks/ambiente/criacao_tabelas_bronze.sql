-- Criação das tabelas na camada bronze apontando para os dados salvos no storage azure

CREATE TABLE IF NOT EXISTS olhovivo_bronze.buscar_linhas 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/buscar_linhas';

-- buscar_linhas_sentido
CREATE TABLE IF NOT EXISTS olhovivo_bronze.buscar_linhas_sentido 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/buscar_linhas_sentido';

-- buscar_paradas
CREATE TABLE IF NOT EXISTS olhovivo_bronze.buscar_paradas 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/buscar_paradas';

-- buscar_paradas_por_corredor
CREATE TABLE IF NOT EXISTS olhovivo_bronze.paradas_por_corredor 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/paradas_por_corredor';

-- buscar_paradas_por_linha
CREATE TABLE IF NOT EXISTS olhovivo_bronze.paradas_por_linha 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/paradas_por_linha';

-- corredores
CREATE TABLE IF NOT EXISTS olhovivo_bronze.corredores 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/corredores';

-- empresas
CREATE TABLE IF NOT EXISTS olhovivo_bronze.empresas 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/empresas';

-- posicao_veiculos
CREATE TABLE IF NOT EXISTS olhovivo_bronze.posicao_veiculos 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/posicao_veiculos';

-- previsao_chegada_parada
CREATE TABLE IF NOT EXISTS olhovivo_bronze.previsao_chegada_parada 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/previsao_chegada_parada';

-- previsao_chegada_parada_linha
CREATE TABLE IF NOT EXISTS olhovivo_bronze.previsao_chegada_linha 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/previsao_chegada_linha';

-- gtfs_routes
CREATE TABLE IF NOT EXISTS olhovivo_bronze.gtfs_routes 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/gtfs_routes';

-- gtfs_stops
CREATE TABLE IF NOT EXISTS olhovivo_bronze.gtfs_stops 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/gtfs_stops';

-- gtfs_agency
CREATE TABLE IF NOT EXISTS olhovivo_bronze.gtfs_agency 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/gtfs_agency';

-- gtfs_trips
CREATE TABLE IF NOT EXISTS olhovivo_bronze.gtfs_trips 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/gtfs_trips';

-- gtfs_shapes
CREATE TABLE IF NOT EXISTS olhovivo_bronze.gtfs_shapes 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/gtfs_shapes';

-- gtfs_fare_attributes
CREATE TABLE IF NOT EXISTS olhovivo_bronze.gtfs_fare_attributes 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/gtfs_fare_attributes';

-- gtfs_fare_rules
CREATE TABLE IF NOT EXISTS olhovivo_bronze.gtfs_fare_rules 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/gtfs_fare_rules';

-- gtfs_calendar
CREATE TABLE IF NOT EXISTS olhovivo_bronze.gtfs_calendar 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/gtfs_calendar';

-- gtfs_frequencies
CREATE TABLE IF NOT EXISTS olhovivo_bronze.gtfs_frequencies 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/gtfs_frequencies';

-- gtfs_stop_times
CREATE TABLE IF NOT EXISTS olhovivo_bronze.gtfs_stop_times
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/gtfs_stop_times';
