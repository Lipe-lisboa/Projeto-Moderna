import pandas as pd


df = pd.read_parquet('arquivos_parquet/2025/Homologacoes_de_janeiro.parquet')

df_filter = df.filter(df['Data da Homologação'] == '1/27/2025 12:00 AM')


print(df_filter)