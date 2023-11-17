import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from scipy import stats

def load_csv(filepath, timestamp_cols=None, encoding_type='utf-8'):
    data = pd.read_csv(filepath, parse_dates=timestamp_cols, encoding=encoding_type)
    return data

def save_csv(data, filepath, encoding="utf8"):
    data.to_csv(filepath, encoding=encoding, index=False)