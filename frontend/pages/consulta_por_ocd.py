import streamlit as st
from utils import API_URL, LISTA_MESES

st.header("Consulta Individual")

ocd_nome = st.text_input("Digite o nome da OCD (Ex: MODERNA, NCC, ICC):").upper()
ano_o = st.number_input("Ano", value=2026,max_value=2026 ,key="ano_o")
mes_o = st.selectbox(
    "Mês",
    LISTA_MESES,
    key="mes_o")

if st.button("Buscar OCD"):
    ...