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
    # df["directors"] = df["directors"].apply(ast.literal_eval)
    df["openDt"] = df["openDt"].astype(str)
    column_list = ["movieCd", "movieNm", "openDt", "typeNm", "nationAlt", "repGenreNm", "directors"]
    df = df[column_list]
    df = df.set_index("movieCd")
    return df

def dataframe_with_selections(df, hide_index=True):
    df_with_selections = df.copy()
    df_with_selections.insert(0, "Select", False)
    edited_df = st.data_editor(
        df_with_selections,
        hide_index=hide_index,
        column_config={"Select": st.column_config.CheckboxColumn(required=True)},
        disabled=df.columns,
        use_container_width=True        
    )
    selected_rows = df[edited_df.Select]
    return selected_rows

# Some number in the range 2018-2023
year_to_filter = st.slider('year', 2018, 2023, 2023)
st.subheader(f'{year_to_filter} Movie List')
data = load_MovieList(str(year_to_filter))
selection = dataframe_with_selections(data)
if len(selection) > 0:
    movieCd = selection.index.values
else:
    movieCd = None

# bar_chart_with_issue(chart_data, issue_info)