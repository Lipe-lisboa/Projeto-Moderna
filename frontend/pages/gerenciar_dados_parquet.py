import streamlit as st
from utils import API_URL
import requests

st.header("Carregar Dados Parquet")

ano = st.number_input("Ano", value=2026,max_value=2026 ,key="ano_p")

if st.button("Carregar Dados Parquet"):
    url_completa = f"{API_URL}/criar-parquets?ano={ano}"

    with st.spinner("Buscando dados na API..."):
        response = requests.get(url_completa)

    if response.status_code == 200:
        st.success(f"Dados de {ano} carregados!")
    else:
        st.error(f"Erro na API: {response.status_code}")