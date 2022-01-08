from omigo_core import tsv
import pandas as pd

# TODO: this needs additional dependencies which have conflict in installation
# fastparquet
# s3fs
def read_parquet(path):
    return tsv.from_df(pd.read_parquet(path))
