🚌 SPTrans Olho Vivo | Databricks Lakehouse (Standard + GTFS)
Pipeline de Engenharia de Dados End-to-End para monitoramento da frota de ônibus de São Paulo. Integra dados de telemetria em tempo real (API Olho Vivo) com dados estáticos de planejamento (GTFS), processados em arquitetura Medallion (Bronze/Silver/Gold) no Databricks e visualizados em um Dashboard Streamlit com Chatbot.

🏗️ Arquitetura e Fluxo de Dados
O projeto foi desenhado para operar com eficiência de custos, utilizando recursos do plano Standard do Databricks na Azure.

```mermaid
graph TD
    %% Definição de Estilos
    classDef config fill:#f9f,stroke:#333,stroke-width:2px;
    classDef bronze fill:#cd7f32,stroke:#333,stroke-width:2px,color:white;
    classDef silver fill:#c0c0c0,stroke:#333,stroke-width:2px;
    classDef gold fill:#ffd700,stroke:#333,stroke-width:2px;
    classDef app fill:#61dafb,stroke:#333,stroke-width:2px;
    classDef source fill:#fff,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5;

    subgraph Fontes_Externas ["Fontes de Dados"]
        API_OlhoVivo["API Olho Vivo SPTrans"]:::source
        GTFS_Files["Arquivos GTFS Estáticos"]:::source
    end

    subgraph Ambiente ["1. Configuração do Ambiente"]
        Schemas["criacao_schemas.sql"]:::config
        Tab_Bronze["criacao_tabelas_bronze.sql"]:::config
        Tab_Silver["criacao_tabelas_silver.sql"]:::config
        Tab_Gold["criacao_tabelas_gold.sql"]:::config
        Tab_GTFS["criacao_tabelas_gtfs.py"]:::config
    end

    subgraph Bronze_Layer ["2. Ingestão (Bronze)"]
        direction TB
        Ingest_Posicao["posicao_veiculos.py"]:::bronze
        Ingest_Linhas["buscar_linhas.py / buscar_linhas_sentido.py"]:::bronze
        Ingest_Paradas["buscar_paradas*.py"]:::bronze
        Ingest_Empresas["empresas.py / corredores.py"]:::bronze
        Ingest_Previsao["previsao_chegada*.py"]:::bronze
    end

    subgraph Silver_Layer ["3. Refinamento (Silver)"]
        direction TB
        Fato_Posicao["fato_posicao_veiculos.py"]:::silver
        Dim_Linhas["dim_linhas.py"]:::silver
        Dim_Empresas["dim_empresas.py"]:::silver
        Mapa_Shapes["mapa_shapes_gtfs.py"]:::silver
    end

    subgraph Gold_Layer ["4. Agregação e KPIs (Gold)"]
        direction TB
        KPI_Velocidade["velocidade_linhas.py"]:::gold
        KPI_Snapshot["snapshot_frota_atual.py"]:::gold
        KPI_Acessibilidade["acessibilidade.py"]:::gold
        KPI_Rota["perfil_rota_estatico.sql"]:::gold
    end

    subgraph Visualization ["5. Visualização"]
        Streamlit_App["app/app.py"]:::app
    end

    %% Conexões
    Schemas --> Tab_Bronze --> Tab_Silver --> Tab_Gold --> Tab_GTFS
    
    API_OlhoVivo --> Ingest_Posicao
    API_OlhoVivo --> Ingest_Linhas
    API_OlhoVivo --> Ingest_Paradas
    API_OlhoVivo --> Ingest_Empresas
    
    Tab_Bronze -.-> Ingest_Posicao
    
    Ingest_Posicao --> Fato_Posicao
    Ingest_Linhas --> Dim_Linhas
    Ingest_Empresas --> Dim_Empresas
    GTFS_Files --> Mapa_Shapes
    
    Fato_Posicao & Dim_Linhas --> KPI_Velocidade
    Fato_Posicao & Dim_Linhas & Dim_Empresas --> KPI_Snapshot
    Dim_Linhas & Fato_Posicao --> KPI_Acessibilidade
    Mapa_Shapes --> KPI_Rota
    
    KPI_Velocidade --> Streamlit_App
    KPI_Snapshot --> Streamlit_App
    KPI_Rota --> Streamlit_App
    KPI_Acessibilidade --> Streamlit_App
```


⚙️ Orquestração (Databricks Workflows)
A automação do pipeline é gerenciada nativamente pelo Databricks Workflows (Jobs), sem necessidade de ferramentas externas como Airflow.

Nome do Job: pipeline_olhovivo

Frequência: A cada 15 minutos (Cron Schedule).

Cluster: Cluster All-Purpose (Standard Mode).

Tasks do Workflow (Execução Sequencial):

1_ing_posic_veic_bronze: Conecta na API e baixa o JSON raw.

2_posic_veic_silver: Processa, explode e limpa os dados.

3_velocidade_gold: Calcula a média de velocidade e tempo de viagem.

4_snapshot_mapa: Atualiza a última posição conhecida da frota.


☁️ Estratégia de Infraestrutura e Custos (FinOps)
Este projeto adota uma arquitetura otimizada para reduzir custos de nuvem e licenciamento Databricks (DBUs), ideal para ambientes de desenvolvimento e POCs.

1. Armazenamento (Azure Storage vs. Catalog)
Dados Físicos (Parquet/Delta): Todos os dados persistem de forma segura em um Azure Storage Account (ADLS Gen2).

Metadados: Utilizamos o Hive Metastore (Legacy) embutido no cluster, ao invés do Unity Catalog, para evitar custos adicionais de gerenciamento e complexidade de setup em workspace Standard.

2. Metadados Efêmeros (Cluster-Scoped)
Como estratégia de economia, utilizamos o metastore local do cluster (banco Derby embutido).

Comportamento: Quando o cluster é desligado/reiniciado, os ponteiros (schemas e definições de tabelas) desaparecem da interface visual do Catalog.

Persistência: Os dados não são perdidos, pois estão salvos no Azure Storage.

Recuperação: O pipeline inclui notebooks de "Ambiente" (criacao_schemas, criacao_tabelas) que recriam os ponteiros apontando para os locais existentes no Storage (LOCATION 'abfss://...') sempre que o ambiente é reiniciado.


🧠 Lógica de Negócio (Camadas)
1. Camada Bronze (Ingestão Raw)
Posições (Real-Time): Conexão autenticada na API da SPTrans.

GTFS (Estático): Ingestão dos arquivos .txt contendo shapes, paradas e viagens.

2. Camada Silver (Limpeza e Modelagem)
Normalização: Flatten de JSONs complexos.

Tipagem: Conversão de coordenadas e timestamps.

Deduplicação: Garante unicidade dos registros de GPS.

3. Camada Gold (Inteligência)
Cálculo Geoespacial: Uso da Fórmula de Haversine para medir a extensão real das linhas (GTFS) e cruzar com a velocidade (GPS) para estimar o tempo de viagem.

Higienização: Filtro de linhas fantasmas (velocidade sem frota ativa) para garantir precisão no dashboard.


📂 Estrutura do Repositório

sptrans-lakehouse/
├── app/
│   └── app.py                     # Dashboard Streamlit + Chatbot
├── databricks_notebooks/
│   ├── ambiente/                  # Setup de Schemas (Recuperação de Metadados)
│   ├── bronze/                    # Ingestão API -> Delta Raw
│   ├── silver/                    # Tratamento e Normalização
│   └── gold/                      # KPIs e Regras de Negócio
├── docs/                          # Documentação auxiliar
├── requirements.txt               # Dependências Python
└── README.md                      # Este arquivo


🚀 Como Executar
Passo a Passo
Setup no Databricks:

Importe os notebooks da pasta databricks_notebooks/.

Execute os scripts da pasta ambiente/ para montar os Schemas no Hive Metastore.

Nota: Se o cluster reiniciar, execute estes scripts novamente para restaurar a visibilidade das tabelas.

Agendamento:

Crie um Job apontando para os notebooks na ordem descrita na seção "Orquestração".

Visualização (Local):

Configure suas credenciais no .streamlit/secrets.toml.

Execute o comando:

streamlit run app/app.py


Desenvolvido como parte do portfólio de Engenharia de Dados com foco em Databricks e Azure.
