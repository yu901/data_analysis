import requests
import json
import pandas as pd
import numpy as np
import datetime
from tqdm import tqdm
from src.main.python.config import KobisConfig
from src.main.python.kobis_path import * 
from src.main.python.utils import *
import ast

kobis_config = KobisConfig()

class Movie():
    def __init__(self):
        self.key = kobis_config.key
    
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
            "key": self.key,
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
            "key": self.key,
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
            "key": self.key,
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
                # movie_y["directors"] = movie_y["directors"].apply(ast.literal_eval)
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
        col_types = {
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
        return boxoffice
    
    def get_MovieBoxOffice(self, movieCd, period=None):
        movie_info = self.request_MovieInfo(movieCd=movieCd)
        openDt = movie_info["openDt"]
        boxoffice = self.get_DailyBoxOffice(openDt, period)
        boxoffice = boxoffice[boxoffice["movieCd"]==movieCd].copy()
        boxoffice = boxoffice.reset_index(drop=True)
        return boxoffice


if __name__ == '__main__':
    movie = Movie()
    # df = movie.get_DailyBoxOffice("20231122", 15)
    df = movie.get_MovieList("2022", 1)
    print(df.head(5))
    # movieCd = "20212866"
    # df = movie.get_MovieBoxOffice(movieCd)
    # print(df[df['movieCd']=="20190549"]['movieNmEn'].values)

    # target_year = "2021"
    # file_path = get_raw_file_path("MovieList", f"MovieList_T{target_year}")
    # df = load_csv(file_path)