import streamlit as st
from utils import LISTA_MESES, API_URL
import requests
import pandas as pd
import plotly.express as px

st.header("Quantidade de Certificados por OCD")

ano_g = st.number_input("Ano", min_value=2020, max_value=2026, value=2026, key="ano_g")
mes_g = st.selectbox(
    "Mês", 
    LISTA_MESES,
    key="mes_g"
)
numero_de_ocds = st.slider("Número de OCDs que deseja visualizar", min_value=1, max_value=25, value=10, key="num_ocds")

if st.button("Consultar Geral"):
# 1. Monta a URL com os parâmetros que o usuário escolheu
    url_completa = f"{API_URL}/certificados?ano={ano_g}&mes={mes_g}"
    
    with st.spinner("Buscando dados na API..."):
        try:
            # 2. Faz a requisição para o Backend
            response = requests.get(url_completa)
            
            if response.status_code == 200:
                dados = response.json()["result"]
                
                # 3. Transforma o JSON em um DataFrame do Pandas para exibir
                df = pd.DataFrame(dados)
                df_total = df.head(numero_de_ocds)
                df_top3 = df.head(3)
                outros_valores = df.iloc[3:].sum()
                
                # 4. Exibe os resultados de forma bonita
                st.success(f"Dados de {mes_g}/{ano_g} carregados!")
                
                # Mostra uma tabela
                st.dataframe(df_total, use_container_width=True)
                
                # Cria um gráfico automático usando a coluna 'ocd' e 'quantidade_de_certificado'
                st.subheader("Gráfico de Quantidade de Certificados por OCD")
                st.bar_chart(
                    data=df_total.set_index("ocd"),
                    x_label="OCD",
                    y_label="Quantidade de Certificados"
                    )
                
                st.subheader(f"Top 3 OCDs com mais certificados")

                # Cria o dataframe para a Pizza
                df_pizza = pd.concat([df_top3, pd.DataFrame([{'ocd': 'OUTROS', 'quantidade_de_certificado': outros_valores['quantidade_de_certificado']}])])

                fig = px.pie(df_pizza, values='quantidade_de_certificado', names='ocd')
                st.plotly_chart(fig)

            else:
                st.error(f"Erro na API: {response.status_code}")
        
        except Exception as e:
            st.error(f"O Backend está desligado? Erro: {e}")