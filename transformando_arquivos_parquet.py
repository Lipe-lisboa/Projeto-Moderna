import pandas as pd

def convert_parquet(df:pd.DataFrame, file:str):
    df.to_parquet(file, index=False)