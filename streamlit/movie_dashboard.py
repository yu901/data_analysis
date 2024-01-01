import streamlit as st
import pandas as pd
import numpy as np
import os.path
import ast
from src.main.python.movie import Movie

st.title('BoxOffice Overview')
movie = Movie()

@st.cache_data
def load_MovieList(year):
    df = movie.get_MovieList(str(year_to_filter))
    df["directors"] = df["directors"].apply(ast.literal_eval)
    df["openDt"] = df["openDt"].astype(str)
    column_list = ["movieCd", "movieNm", "openDt", "typeNm", "nationAlt", "repGenreNm", "directors"]
    df = df[column_list]
    df = df.set_index("movieCd")
    return df

# Some number in the range 2018-2023
year_to_filter = st.slider('year', 2018, 2023, 2023)
st.subheader(f'{year_to_filter} Movie List')
data = load_MovieList(str(year_to_filter))
st.write(data)