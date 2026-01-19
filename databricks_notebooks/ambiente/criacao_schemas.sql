-- No plano Standard, usamos o underscore (_) para separar projeto e camada

CREATE SCHEMA IF NOT EXISTS olhovivo_bronze
LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/';

CREATE SCHEMA IF NOT EXISTS olhovivo_silver
LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/silver/';

CREATE SCHEMA IF NOT EXISTS olhovivo_gold
LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/gold/';
