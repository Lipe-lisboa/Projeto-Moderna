import streamlit as st
from utils import API_URL, LISTA_MESES

st.header("Consulta Individual")

ano = st.number_input("Ano", value=2026,max_value=2026 ,key="ano_p")
periodo_expecifico = st.checkbox("Consultar período específico", key="periodo_expecifico")

if periodo_expecifico:
    mes_inicial = st.selectbox(
        "Mês Inicial",
        LISTA_MESES,
        key="mes_inicial"
    )

    mes_final = st.selectbox(
        "Mês Final",
        LISTA_MESES,
        key="mes_final"
    )

if st.button("Carregar Dados Parquet"):
    ...