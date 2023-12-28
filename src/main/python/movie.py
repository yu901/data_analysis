import requests
import json
import pandas as pd
import numpy as np
import datetime
from tqdm import tqdm
from config import KobisConfig
from utils import *

kobis_config = KobisConfig()

class Movie():
    def __init__(self):
        self.key = kobis_config.key

    def get_dir_path(self):
        dir_path = kobis_config.data
        return dir_path

    def get_csv_file_path(self, file_name):
        dir_path = self.get_dir_path()
        return f"{dir_path}/{file_name}.csv"
    
    def save_data(self, data, file_name):
        dir_path = self.get_dir_path()
        file_path = self.get_csv_file_path(file_name)
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

    def get_MovieList(self, openStartDt, period=0):
        movie_list = pd.DataFrame()
        curPage = 1
        list_exist = True
        while list_exist:
            openEndDt = str(int(openStartDt) + period)
            df, list_exist = self.request_MovieList(curPage, openStartDt, openEndDt)      
            movie_list = pd.concat([movie_list, df], ignore_index=True)
            curPage += 1
        movie_list["directors_str"] = movie_list["directors"].astype(str)
        movie_list = movie_list[
            (movie_list["repGenreNm"]!="성인물(에로)") & 
            (movie_list["movieNmEn"]!="") &
            (movie_list["directors_str"]!="[]")].copy()
        movie_list = movie_list.drop(columns=["directors_str"])
        self.save_data(movie_list, f"MovieList_S{openStartDt}_E{openEndDt}")
        return movie_list

    def get_DailyBoxOffice(self, startDt, period=None):
        extract_range = self.get_extract_range(startDt, period)
        boxoffice = pd.DataFrame()
        for extract_date in tqdm(extract_range):
            df = self.request_DailyBoxOffice(extract_date)
            df["targetDt"] = extract_date[:4] + "-" + extract_date[4:6] + "-" + extract_date[6:]
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
        self.save_data(boxoffice, f"BoxOffice_S{startDt}_E{extract_date}")
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
    # df = movie.get_DailyBoxOffice("20231122", 10)
    df = movie.get_MovieList("2020")
    movieCd = "20212866"
    # df = movie.get_MovieBoxOffice(movieCd)
    print(df[df['movieCd']=="20190549"]['movieNmEn'].values)