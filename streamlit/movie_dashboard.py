import streamlit as st
import pandas as pd
import numpy as np
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm # 폰트매니저
from src.main.python.movie import Movie

st.set_option('deprecation.showPyplotGlobalUse', False)
st.title('BoxOffice Overview')
movie = Movie()

path = './fonts/NanumBarunGothic.ttf' # NanumBarunGothic 폰트의 경로를 지정
fe = fm.FontEntry(
    fname=path, # ttf 파일이 저장되어 있는 경로ㅣ
    name='NanumGothic')                        # 이 폰트의 원하는 이름 설정
fm.fontManager.ttflist.insert(0, fe)              # Matplotlib에 폰트 추가
plt.rcParams.update({'font.size': 18, 'font.family': 'NanumGothic'})
# Create and generate a word cloud image:
wc = WordCloud(
    font_path = path,
    width=1000,  # 그래프 크기
    height=1000, # 그래프 크기
    scale=3.0, # 해상도 스케일링
    max_font_size=250 # 최대 폰트

)

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

def line_chart_with_movieCd(movieCds):
    if (np.array(movieCds == None)).all():
        st.text("Select a movie from the list.")
    else:
        boxoffice = movie.get_MoviesBoxOffice(movieCds, period=60)
        st.line_chart(boxoffice, x="elapsedDt", y="audiCnt", color="movieNm")

def word_cloud_with_movieNm(movieNm):
    if (np.array(movieNm == None)).all():
        st.text("Select a movie from the list.")
    else:
        keywords = movie.get_movie_words(movieNm)
        gen = wc.generate_from_frequencies(keywords)
        # Display the generated image:
        plt.imshow(gen, interpolation='bilinear')
        plt.axis("off")
        plt.show()
        st.pyplot()


# Some number in the range 2018-2023
year_to_filter = st.slider('year', 2018, 2024, 2024)
st.subheader(f'{year_to_filter} Movie List')
data = load_MovieList(str(year_to_filter))
selection = dataframe_with_selections(data)
if len(selection) > 0:
    movieCds = selection.index.values
    movieNm = selection.iloc[0]['movieNm']
else:
    movieCds = None
    movieNm = None

# movieCds = ["20212866", "20234675"]
line_chart_with_movieCd(movieCds)
word_cloud_with_movieNm(movieNm)