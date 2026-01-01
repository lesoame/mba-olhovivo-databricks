import streamlit as st
from databricks import sql
import pandas as pd
import plotly.express as px

# --- CONFIGURAÇÃO DA PÁGINA (Deve ser a primeira linha) ---
st.set_page_config(
    page_title="Olho Vivo - Monitoramento SPTrans",
    page_icon="🚌",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- ESTILO CSS PERSONALIZADO (Para dar um visual profissional) ---
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 15px;
        text-align: center;
    }
    div[data-testid="stMetricValue"] {
        font-size: 28px;
        color: #002F6C; /* Azul SPTrans */
    }
</style>
""", unsafe_allow_html=True)

# --- CREDENCIAIS DATABRICKS ---
# Dica: Em produção, usaríamos st.secrets, mas hardcode funciona pro MBA.
SERVER_HOSTNAME = st.secrets["databricks"].["server_hostname"]
HTTP_PATH = st.secrets["databricks"]["http_path"]
ACCESS_TOKEN = st.secrets["databricks"]["access_token"]

# --- FUNÇÃO DE CONEXÃO COM CACHE ---
# O TTL=300 significa que ele segura os dados por 5 min antes de ir no banco de novo
@st.cache_data(ttl=300)
def get_data(query):
    try:
        conn = sql.connect(
            server_hostname=SERVER_HOSTNAME,
            http_path=HTTP_PATH,
            access_token=ACCESS_TOKEN
        )
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Erro na conexão com Databricks: {e}")
        return pd.DataFrame()

# --- SIDEBAR: FILTROS E CONTROLES ---
st.sidebar.image("https://www.sptrans.com.br/img/logo_sptrans.png", width=150)
st.sidebar.header("🎛️ Painel de Controle")

# Botão de Atualizar
if st.sidebar.button("🔄 Atualizar Dados Agora"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("---")

# --- CARREGAMENTO DOS DADOS (GOLD) ---
with st.spinner('Conectando ao Data Lakehouse...'):
    # 1. Mapa (Snapshot Atual)
    df_mapa = get_data("SELECT * FROM olhovivo_silver.gold_snapshot_frota_atual")
    
    # 2. Velocidade (Gargalos)
    df_vel = get_data("SELECT * FROM olhovivo_silver.gold_velocidade_linhas")
    
    # 3. Acessibilidade
    df_acess = get_data("SELECT * FROM olhovivo_silver.gold_acessibilidade")

# --- FILTROS DINÂMICOS ---
# Criamos uma lista de linhas baseada nos dados carregados
if not df_mapa.empty:
    linhas_disponiveis = sorted(df_mapa['cod_linha'].unique())
    # Adiciona opção "TODAS"
    linha_selecionada = st.sidebar.selectbox(
        "Filtrar por Código da Linha:", 
        options=["TODAS"] + list(map(str, linhas_disponiveis))
    )
else:
    linha_selecionada = "TODAS"

# Aplicando Filtro nos DataFrames
if linha_selecionada != "TODAS":
    df_mapa = df_mapa[df_mapa['cod_linha'] == int(linha_selecionada)]
    df_vel = df_vel[df_vel['cod_linha'] == int(linha_selecionada)]
    df_acess = df_acess[df_acess['cod_linha'] == int(linha_selecionada)]

# --- CABEÇALHO PRINCIPAL ---
st.title("🚌 Centro de Controle Operacional - MBA")
st.markdown(f"**Visão em Tempo Real** | Filtro Atual: `{linha_selecionada}`")
st.markdown("---")

# --- LINHA 1: KPIs (INDICADORES) ---
kpi1, kpi2, kpi3, kpi4 = st.columns(4)

total_onibus = len(df_mapa)
vel_media = df_vel['velocidade_media'].mean() if not df_vel.empty else 0
acessibilidade = df_acess['percentual_acessibilidade'].mean() if not df_acess.empty else 0

kpi1.metric("Frota Ativa", f"{total_onibus}", delta="Veículos")
kpi2.metric("Velocidade Média", f"{vel_media:.1f} km/h", delta_color="normal")
kpi3.metric("Índice Acessibilidade", f"{acessibilidade:.1f}%", delta="Meta: 100%")
kpi4.metric("Status do Pipeline", "Online 🟢") # Simulado para efeito visual

st.markdown("---")

# --- LINHA 2: MAPA E GRÁFICOS ---
col_mapa, col_graficos = st.columns([2, 1]) # Mapa ocupa 2/3 da tela

with col_mapa:
    st.subheader(f"📍 Monitoramento Geográfico ({len(df_mapa)} veículos)")
    if not df_mapa.empty:
        # Mapa Plotly (Mais bonito e interativo que o st.map)
        fig_mapa = px.scatter_mapbox(
            df_mapa,
            lat="latitude",
            lon="longitude",
            color="sentido_desc", # Pinta Ida de uma cor, Volta de outra
            hover_name="prefixo_veiculo",
            hover_data={"velocidade_kmh": True, "cod_linha": True},
            zoom=10,
            height=500,
            color_discrete_sequence=["#00cc96", "#ef553b"] # Verde e Vermelho
        )
        # Estilo OpenStreetMap (Não precisa de token)
        fig_mapa.update_layout(mapbox_style="open-street-map")
        fig_mapa.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig_mapa, use_container_width=True)
    else:
        st.warning("Sem dados de GPS no momento.")

with col_graficos:
    st.subheader("📊 Gargalos (Mais Lentas)")
    
    if not df_vel.empty:
        # Pega as 10 mais lentas
        top_lentas = df_vel.sort_values(by="velocidade_media", ascending=True).head(10)
        
        fig_bar = px.bar(
            top_lentas,
            x="velocidade_media",
            y="letreiro_completo",
            orientation='h',
            text_auto='.1f',
            color="velocidade_media",
            color_continuous_scale="Reds_r" # Vermelho escuro para lento
        )
        fig_bar.update_layout(
            yaxis=dict(autorange="reversed"), # Mais lento no topo
            xaxis_title="Km/h",
            yaxis_title="",
            showlegend=False,
            height=500
        )
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("Sem dados de velocidade para exibir.")

# --- RODAPÉ ---
st.markdown("---")
st.caption("Desenvolvido para o MBA - Engenharia de Dados com Databricks & Streamlit")
