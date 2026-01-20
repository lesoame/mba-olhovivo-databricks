import streamlit as st
from databricks import sql
import pandas as pd
import plotly.express as px
import numpy as np
import time

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="MBA Olho Vivo | Assistente Virtual",
    page_icon="🧛", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CSS: TEMA DRACULA ---
st.markdown("""
<style>
    .stApp { background-color: #282a36; color: #f8f8f2; }
    section[data-testid="stSidebar"] { background-color: #44475a; color: #f8f8f2; }
    h1, h2, h3, p, label, .stMarkdown, .stText { color: #f8f8f2 !important; }
    div[data-testid="stMetricValue"] { color: #bd93f9 !important; font-weight: bold; }
    div[data-testid="stMetricLabel"] { color: #ff79c6 !important; }
    div[data-testid="stDataFrame"] { background-color: #282a36; }
    .stTextInput > div > div > input { background-color: #6272a4; color: white; }
    hr { border-color: #6272a4; }
</style>
""", unsafe_allow_html=True)

# --- CONEXÃO DATABRICKS ---
try:
    SERVER_HOSTNAME = st.secrets["databricks"]["server_hostname"]
    HTTP_PATH = st.secrets["databricks"]["http_path"]
    ACCESS_TOKEN = st.secrets["databricks"]["access_token"]
except KeyError:
    st.error("Erro: Credenciais não configuradas no .streamlit/secrets.toml")
    st.stop()

@st.cache_data(ttl=60)
def get_data(query):
    try:
        conn = sql.connect(server_hostname=SERVER_HOSTNAME, http_path=HTTP_PATH, access_token=ACCESS_TOKEN)
        cursor = conn.cursor()
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        conn.close()
        return df
    except Exception as e:
        st.error(f"Erro Databricks: {e}")
        return pd.DataFrame()

# --- CARGA DE DADOS ---
with st.spinner('🦇 Carregando Pipeline Gold...'):
    df_vel_full = get_data("""
        SELECT cod_linha, letreiro_completo, nome_rota_oficial, velocidade_media, sentido_desc, data_referencia 
        FROM olhovivo_gold.velocidade_linhas
    """)
    
    df_mapa_full = get_data("""
        SELECT cod_linha, prefixo_veiculo, latitude, longitude, sentido_desc, is_acessivel, data_hora_sp, data_referencia 
        FROM olhovivo_gold.snapshot_frota_atual
    """)
    
    df_estatico = get_data("""
        SELECT letreiro_gtfs, extensao_km, qtd_paradas_estimada 
        FROM olhovivo_gold.perfil_rota_estatico
    """)

# --- ETL BLINDADO ---
if not df_vel_full.empty:
    # 1. Tratamento de Strings
    df_vel_full['letreiro_completo'] = df_vel_full['letreiro_completo'].fillna('?')
    df_vel_full['nome_rota_oficial'] = df_vel_full['nome_rota_oficial'].fillna('Desc. Indisponível')
    df_vel_full['Rota'] = df_vel_full['letreiro_completo'].astype(str) + " | " + df_vel_full['nome_rota_oficial'].astype(str)
    
    # 2. PADRONIZAÇÃO DE CHAVES
    df_vel_full['cod_linha'] = df_vel_full['cod_linha'].astype(str).str.split('.').str[0].str.strip()
    df_vel_full['data_referencia'] = df_vel_full['data_referencia'].astype(str)
    
    if not df_mapa_full.empty:
        df_mapa_full['cod_linha'] = df_mapa_full['cod_linha'].astype(str).str.split('.').str[0].str.strip()
        df_mapa_full['data_referencia'] = df_mapa_full['data_referencia'].astype(str)

    # 3. Tipagem Numérica
    df_vel_full['velocidade_media'] = pd.to_numeric(df_vel_full['velocidade_media'], errors='coerce')

# --- SIDEBAR ---
st.sidebar.markdown("### 🧛 Painel de Controle")

if not df_vel_full.empty:
    datas = sorted(df_vel_full['data_referencia'].unique(), reverse=True)
    data_sel = st.sidebar.selectbox("Data:", datas)
else:
    data_sel = None

if data_sel:
    df_vel = df_vel_full[df_vel_full['data_referencia'] == data_sel].copy()
    df_mapa = df_mapa_full[df_mapa_full['data_referencia'] == data_sel].copy()
else:
    df_vel, df_mapa = pd.DataFrame(), pd.DataFrame()

st.sidebar.markdown("---")
so_acessiveis = st.sidebar.checkbox("Apenas Frota Acessível ♿")

# --- MERGE & ENGENHARIA ---
if not df_vel.empty:
    # Agrupa Frota
    if not df_mapa.empty:
        df_agg = df_mapa.groupby('cod_linha').agg(
            Qtd_Veiculos=('prefixo_veiculo', 'count'),
            Acessiveis=('is_acessivel', lambda x: (x.astype(str).isin(['true','True','1'])).sum())
        ).reset_index()
    else:
        df_agg = pd.DataFrame(columns=['cod_linha', 'Qtd_Veiculos', 'Acessiveis'])

    # Join Principal
    df_final = pd.merge(df_vel, df_agg, on='cod_linha', how='left')
    
    # Join GTFS
    if not df_estatico.empty:
        df_final = pd.merge(df_final, df_estatico, left_on='letreiro_completo', right_on='letreiro_gtfs', how='left')
        df_final['extensao_km'] = df_final['extensao_km'].astype(float).fillna(df_estatico['extensao_km'].mean())
    else:
        df_final['extensao_km'] = 15.0

    # === CÁLCULOS ===
    df_final['Qtd_Veiculos'] = df_final['Qtd_Veiculos'].fillna(0).astype(int)
    
    # Tempo Estimado
    df_final['tempo_estimado_h'] = df_final['extensao_km'].div(df_final['velocidade_media'].replace(0, np.nan))
    
    # Acessibilidade
    df_final['Perc_Acessivel'] = (
        df_final['Acessiveis'].div(df_final['Qtd_Veiculos'].replace(0, np.nan))
        .fillna(0) * 100
    )
    
    df_final['Status_Acessibilidade'] = df_final.apply(
        lambda x: "✅" if x['Perc_Acessivel'] >= 100 and x['Qtd_Veiculos'] > 0 
        else ("⚠️" if x['Perc_Acessivel'] > 0 else "🚫"), axis=1
    )

    if so_acessiveis:
        df_final = df_final[df_final['Perc_Acessivel'] > 0]
    
    # === HIGIENIZAÇÃO (CRÍTICO PARA APRESENTAÇÃO) ===
    # 1. Remove nomes inválidos
    # 2. Remove Linhas com Frota zerada (para evitar inconsistência visual)
    df_final = df_final[
        (df_final['letreiro_completo'] != '?') & 
        (df_final['nome_rota_oficial'] != 'Desc. Indisponível') &
        (df_final['Qtd_Veiculos'] > 0) # <--- AQUI ESTÁ A CORREÇÃO
    ]
else:
    df_final = pd.DataFrame()

# ==============================================================================
# DASHBOARD
# ==============================================================================

st.title("🚌 SPTrans API Olho Vivo")
st.markdown(f"**Status:** Pipeline Gold Online | **Data:** {data_sel}")

if df_mapa.empty and data_sel:
    st.warning(f"⚠️ Atenção: Filtro de data falhou ou não há dados de GPS para {data_sel}.")

st.divider()

# KPIs
k1, k2, k3, k4 = st.columns(4)
k1.metric("Linhas Monitoradas", len(df_final))
k2.metric("Frota Ativa", int(df_final['Qtd_Veiculos'].sum())) 
k3.metric("Vel. Média Global", f"{df_final['velocidade_media'].mean():.1f} km/h")
k4.metric("Extensão da Rede", f"{df_final['extensao_km'].sum():,.0f} km")

st.divider()

# --- GRÁFICOS ---
st.subheader("📊 Ranking de Performance")
c1, c2 = st.columns(2)

if not df_final.empty:
    df_chart = df_final.dropna(subset=['velocidade_media']).copy()
    # Agrupa por rota tirando a média
    df_chart = df_chart.groupby('Rota', as_index=False)['velocidade_media'].mean()
else:
    df_chart = pd.DataFrame(columns=['Rota', 'velocidade_media'])

with c1:
    if not df_chart.empty:
        # LENTAS
        top_lentas = df_chart.sort_values('velocidade_media', ascending=True).head(10)
        
        fig = px.bar(top_lentas, x='velocidade_media', y='Rota', orientation='h', 
                     text_auto='.1f',
                     color='velocidade_media', color_continuous_scale=['#ff5555', '#ffb86c']) 
        
        fig.update_layout(
            title_text="🐢 Linhas Mais Lentas",
            title_x=0.5,
            title_font_size=24,
            yaxis=dict(autorange="reversed"),
            yaxis_title="", 
            xaxis_title="Km/h", 
            coloraxis_showscale=False, 
            plot_bgcolor='#282a36', 
            paper_bgcolor='#282a36', 
            font_color='#f8f8f2', 
            height=450
        )
        st.plotly_chart(fig, use_container_width=True)

with c2:
    if not df_chart.empty:
        # RÁPIDAS
        top_rapidas = df_chart.sort_values('velocidade_media', ascending=False).head(10)
        
        fig = px.bar(top_rapidas, x='velocidade_media', y='Rota', orientation='h', 
                     text_auto='.1f',
                     color='velocidade_media', color_continuous_scale=['#8be9fd', '#50fa7b']) 
        
        fig.update_layout(
            title_text="🐇 Linhas Mais Rápidas",
            title_x=0.5,
            title_font_size=24,
            yaxis=dict(autorange="reversed"),
            yaxis_title="", 
            xaxis_title="Km/h", 
            coloraxis_showscale=False,
            plot_bgcolor='#282a36', 
            paper_bgcolor='#282a36', 
            font_color='#f8f8f2', 
            height=450
        )
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# TABELA
st.subheader("📋 Detalhes da Operação")
st.dataframe(
    df_final[['Rota', 'velocidade_media', 'extensao_km', 'tempo_estimado_h', 'Qtd_Veiculos', 'Status_Acessibilidade']].sort_values('velocidade_media'),
    column_config={
        "Rota": st.column_config.TextColumn("Nome da Linha", width="large"),
        "velocidade_media": st.column_config.NumberColumn("Velocidade Média em km/h", format="%.1f"),
        "extensao_km": st.column_config.NumberColumn("Distância da Rota", format="%.1f km"),
        "tempo_estimado_h": st.column_config.NumberColumn("Tempo médio de conclusão", format="%.1f h"),
        "Qtd_Veiculos": st.column_config.NumberColumn("Frota", format="%d"),
        "Status_Acessibilidade": st.column_config.TextColumn("Acessibilidade", width="small"),
    },
    use_container_width=True,
    hide_index=True
)

st.divider()

# CHATBOT
st.subheader("💬 Assistente de Ponto de Ônibus")
st.caption("Pergunte sobre qualquer linha e receba a previsão em tempo real.")

if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "assistant", "content": "Olá! Qual linha você está esperando? (Ex: Digite '9162')"}]

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if prompt := st.chat_input("Digite o número ou nome da linha..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        
        resultado = df_final[df_final['Rota'].str.contains(prompt, case=False, na=False)]
        
        if not resultado.empty:
            linha = resultado.iloc[0]
            nome = linha['Rota']
            vel = linha['velocidade_media']
            frota = linha['Qtd_Veiculos']
            
            if vel and vel > 0:
                tempo_1km = (1 / vel) * 60 
                tempo_5km = (5 / vel) * 60 
                txt_chegada = f"""
                🕒 **Previsão de Chegada:**
                - Se estiver perto (1km): **~{int(tempo_1km)} min**
                - Se estiver longe (5km): **~{int(tempo_5km)} min**
                """
            else:
                txt_chegada = "⚠️ Linha parada ou sem dados de velocidade no momento."

            resposta = f"""
            **Linha Encontrada:** {nome} 🚌
            
            - **Frota Ativa:** {int(frota)} ônibus
            - **Velocidade Agora:** {vel:.1f} km/h
            
            {txt_chegada}
            """
        else:
            resposta = f"Não encontrei nenhuma linha ativa hoje com o termo '{prompt}'. Tente outro número."
        
        message_placeholder.markdown(resposta)
        st.session_state.messages.append({"role": "assistant", "content": resposta})
