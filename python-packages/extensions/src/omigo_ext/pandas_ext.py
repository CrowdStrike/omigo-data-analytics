from omigo_core import tsv
import pandas as pd

def read_parquet(path):
    return tsv.from_df(pd.read_parquet(path))
