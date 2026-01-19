-- Criação das tabelas na camada gold apontando para os dados salvos no storage da azure

-- acessibilidade
CREATE TABLE IF NOT EXISTS olhovivo_gold.acessibilidade
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/gold/acessibilidade';

-- velocidade_linhas
CREATE TABLE IF NOT EXISTS olhovivo_gold.velocidade_linhas
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/gold/velocidade_linhas';

-- snapshot_frota_atual
CREATE TABLE IF NOT EXISTS olhovivo_gold.snapshot_frota_atual
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/gold/snapshot_frota_atual';

