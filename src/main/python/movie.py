import requests
import json
import pandas as pd
import numpy as np
import datetime
from tqdm import tqdm
import urllib.request
import re
from krwordrank.word import KRWordRank
from src.main.python.config import KobisConfig, NaverConfig
from src.main.python.kobis_path import * 
from src.main.python.utils import *
import ast

kobis_config = KobisConfig()
naver_config = NaverConfig()

class Movie():
    def __init__(self):
        self.kobis_key = kobis_config.key
        self.naver_client_id = naver_config.client_id
        self.naver_client_secret = naver_config.client_secret

    def save_data(self, data, file_path):
        dir_path = os.path.dirname(os.path.abspath(file_path))
        if not os.path.isdir(dir_path):
            os.makedirs(dir_path)
        save_csv(data, file_path)

    def get_extract_range(self, startDt, period=None):
        f = "%Y%m%d"
        start_time = datetime.datetime.strptime(startDt, f)
        now_time = datetime.datetime.now() - datetime.timedelta(days=1)
        if period == None:
            limit_time = now_time
        else:
            limit_time = min(now_time, start_time + datetime.timedelta(days=period))
        extract_range = []
        extract_time = start_time
        while extract_time < limit_time:
            extract_str = extract_time.strftime(f)
            extract_range.append(extract_str)
            extract_time += datetime.timedelta(days=1)            
        return extract_range

    def request_DailyBoxOffice(self, targetDt):
        # 일별 박스오피스
        url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
        params = {
            "key": self.kobis_key,
            "targetDt": targetDt
        }    
        while True:
            try:
                response = requests.get(url, params=params)
                text = response.text
                loads = json.loads(text)
                df = pd.DataFrame(loads["boxOfficeResult"]["dailyBoxOfficeList"])
            except:
                continue
            break
        return df
    
    def request_MovieInfo(self, movieCd):
        # 영화 상세정보
        url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"
        params = {
            "key": self.kobis_key,
            "movieCd": movieCd
        }
        while True:
            try:
                response = requests.get(url, params=params)
                text = response.text
                loads = json.loads(text)
                movie_info = loads["movieInfoResult"]["movieInfo"]
            except:
                continue
            break
        return movie_info

    def request_MovieList(self, curPage, openStartDt, openEndDt):
        # 영화 목록
        url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json"
        params = {
            "key": self.kobis_key,
            "itemPerPage": "100",
            "curPage": str(curPage),
            "openStartDt": openStartDt,
            "openEndDt": openEndDt
        }  
        list_exist = True
        while True:
            try:
                response = requests.get(url, params=params)
                text = response.text
                loads = json.loads(text)
                df = pd.DataFrame(loads["movieListResult"]["movieList"])
            except:
                continue
            break            
        if loads["movieListResult"]["totCnt"] == 0:
            list_exist = False 
        return df, list_exist

    def get_MovieList(self, openStartDt, period=1):
        movie_list = pd.DataFrame()
        target_year = openStartDt
        for years in range(period):
            target_year = str(int(target_year) + years)
            file_path = get_raw_file_path("MovieList", f"MovieList_T{target_year}")
            if not os.path.isfile(file_path):
                movie_y = pd.DataFrame()
                curPage = 1
                list_exist = True
                while list_exist:
                    movie_p, list_exist = self.request_MovieList(curPage, target_year, target_year)      
                    movie_y = pd.concat([movie_y, movie_p], ignore_index=True)
                    curPage += 1
                movie_y["directors"] = movie_y["directors"].apply(lambda x: [director["peopleNm"] for director in x])
                movie_y["directors_str"] = movie_y["directors"].astype(str)
                movie_y = movie_y[
                    (movie_y["repGenreNm"]!="성인물(에로)") & 
                    (movie_y["movieNmEn"]!="") &
                    (movie_y["directors_str"]!="[]")].copy()
                movie_y = movie_y.drop(columns=["directors_str"])
                self.save_data(movie_y, file_path)
                print(f"{file_path.split('/')[-1]} is saved")
            else:
                movie_y = load_csv(file_path)
                movie_y["directors"] = movie_y["directors"].apply(ast.literal_eval)
                print(f"{file_path.split('/')[-1]} already exists")
            movie_list = pd.concat([movie_list, movie_y], ignore_index=True)
        return movie_list

    def get_DailyBoxOffice(self, startDt, period=None):
        extract_range = self.get_extract_range(startDt, period)
        boxoffice = pd.DataFrame()
        for extract_date in extract_range:
            file_path = get_raw_file_path("DailyBoxOffice", f"DailyBoxOffice_T{extract_date}")
            if not os.path.isfile(file_path):
                df = self.request_DailyBoxOffice(extract_date)
                df["targetDt"] = extract_date[:4] + "-" + extract_date[4:6] + "-" + extract_date[6:]
                self.save_data(df, file_path)
                print(f"{file_path.split('/')[-1]} is saved")
            else:
                df = load_csv(file_path)
                print(f"{file_path.split('/')[-1]} already exists")
            boxoffice = pd.concat([boxoffice, df], ignore_index=True)
        boxoffice = boxoffice[boxoffice["openDt"]!=" "].copy()
        col_types = {
            "movieCd": "str",
            "salesAmt": "float",
            "salesShare": "float",
            "salesInten": "float",
            "salesChange": "float",
            "salesAcc": "float",
            "audiCnt": "float",
            "audiInten": "float",
            "audiChange": "float",
            "audiAcc": "float",
            "scrnCnt": "float",
            "showCnt": "float",
            "openDt": "datetime64[ns]",
            "targetDt": "datetime64[ns]",
        }
        boxoffice = boxoffice.astype(col_types)
        boxoffice["elapsedDt"] = (boxoffice["targetDt"] - boxoffice["openDt"]) / np.timedelta64(1, 'D')
        boxoffice["elapsedDt"] = boxoffice["elapsedDt"].astype(int)
        return boxoffice
    
    def get_MovieBoxOffice(self, movieCd, period=None):
        movie_info = self.request_MovieInfo(movieCd=movieCd)
        openDt = movie_info["openDt"]
        boxoffice = self.get_DailyBoxOffice(openDt, period)
        boxoffice = boxoffice[boxoffice["movieCd"]==movieCd].copy()
        boxoffice = boxoffice.reset_index(drop=True)
        return boxoffice

    def get_MoviesBoxOffice(self, movieCds, period=None):
        boxoffice = pd.DataFrame()
        for movieCd in movieCds:
            df = self.get_MovieBoxOffice(movieCd=movieCd, period=period)
            boxoffice = pd.concat([boxoffice, df], ignore_index=True)
        return boxoffice
    
    def request_webkr(self, keyword):
        encText = urllib.parse.quote(keyword)
        results = ""
        for i in range(10):
            start = 100 * i + 1
            url = "https://openapi.naver.com/v1/search/webkr?query=" + encText +f"&display=100&start={start}"# JSON 결과
            request = urllib.request.Request(url)
            request.add_header("X-Naver-Client-Id",self.naver_client_id)
            request.add_header("X-Naver-Client-Secret",self.naver_client_secret)
            response = urllib.request.urlopen(request)
            rescode = response.getcode()
            if(rescode==200):
                response_body = response.read()
                result = response_body.decode('utf-8')
            else:
                result = ''
                print("Error Code:" + rescode)
            results += result
        return results

    def get_movie_words(self, movie_nm):
        # Regex Description
        results = self.request_webkr(movie_nm)
        results = re.sub("(<b>|<\\\/b>|&lt|&gt|;|:|\(|\)|\[|\]|\"|\'|\.|\‘|\’|\,|\《|\》|\〈|\〉)", "", results)
        result_list = re.findall("description([^\n]*)", results)
        result_list = [result for result in result_list if ("영화" in result)|(movie_nm in result)]
        
        # Word Rank
        min_count = 5 # 단어출현빈도 수 (5번 이상 출현한 단어)
        max_length = 10 # 단어 길이 최대 값
        verbose = True
        wordrank_extractor = KRWordRank(min_count=min_count, max_length=max_length, verbose=verbose)
        beta = 0.85  # PageRank의 decaying factor beta, 이 값을 이용하여 각 노드(키워드) 간의 관계를 계산
        max_iter = 10 # WordRank 알고리즘의 최대 반복 횟수
        keywords, rank, graph = wordrank_extractor.extract(result_list, beta, max_iter)
        stopwords=['개봉', '영화', '배우', '관객', '감독', '있는', '있다', '이후', '뉴스레터', '문서', '다룬', '그린', '누적', '기준', '구독하기', '다시보', '전체', '지난', '있습니다', '대한', '무료', '버전', '다시', '보기', '있었', '따르면', '다운', '있다', '등으로', '등이', '정보', '출연', '따르면', '오는', 'CGV', '메가박스', '롯데시네마'] # 위 keywords에서 제거하고싶은 단어
        #stopwords 제거 후 keywords 상위 300개 추출
        passwords={
            word:score for word, score in sorted(
                keywords.items(), key=lambda x:-x[1])[:300] if not (word in stopwords)
        }
        return passwords


if __name__ == '__main__':
    movie = Movie()
    # df = movie.get_DailyBoxOffice("20231122", 15)
    # df = movie.get_MovieList("2022", 1)
    # print(df.head(5))
    # 서울의 봄: 20212866 / 슬램덩크: 20228555
    # movieCd = "20228555"
    # df = movie.get_MovieBoxOffice(movieCd)
    # print(df)
    # print(df[df['movieCd']=="20190549"]['movieNmEn'].values)

    # target_year = "2021"
    # file_path = get_raw_file_path("MovieList", f"MovieList_T{target_year}")
    # df = load_csv(file_path)
    print(movie.get_movie_words("서울의 봄"))