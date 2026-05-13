import streamlit as st
from utils import LISTA_MESES, API_URL
import requests
import pandas as pd

st.header("Quantidade de Certificados por OCD")

ano_g = st.number_input("Ano", min_value=2020, max_value=2026, value=2026, key="ano_g")
mes_g = st.selectbox(
    "Mês", 
    LISTA_MESES,
    key="mes_g"
)

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
                df = pd.DataFrame(dados).head(25)
                
                # 4. Exibe os resultados de forma bonita
                st.success(f"Dados de {mes_g}/{ano_g} carregados!")
                
                # Mostra uma tabela
                st.dataframe(df, use_container_width=True)
                
                # Cria um gráfico automático usando a coluna 'ocd' e 'quantidade_de_certificado'
                st.subheader("Gráfico de Quantidade de Certificados por OCD")
                st.bar_chart(
                    data=df.set_index("ocd"),
                    x_label="OCD",
                    y_label="Quantidade de Certificados"
                    )
                
            else:
                st.error(f"Erro na API: {response.status_code}")
        
        except Exception as e:
            st.error(f"O Backend está desligado? Erro: {e}")