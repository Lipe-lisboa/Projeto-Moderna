import streamlit as st
from utils import LISTA_MESES, API_URL

st.header("Quantidade de Certificados por OCD")

ano_g = st.number_input("Ano", min_value=2020, max_value=2026, value=2026, key="ano_g")
mes_g = st.selectbox(
    "Mês", 
    LISTA_MESES,
    key="mes_g"
)

if st.button("Consultar Geral"):
    ...