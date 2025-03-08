import pandas as pd

def convert_parquet(df:pd.DataFrame, nome):
    df.to_parquet(nome, index=False)