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

    def get_extract_range(self, startDt, period):
        f = "%Y%m%d"
        start_date = datetime.datetime.strptime(startDt, f)
        extract_range = []
        for days in range(period):
            extract_date = start_date + datetime.timedelta(days=days)
            if extract_date < datetime.datetime.now():
                extract_date = extract_date.strftime(f)
                extract_range.append(extract_date)
            else:
                breakpoint
        return extract_range

    def request_DailyBoxOffice(self, targetDt):
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

    def request_MovieList(self, curPage, openStartDt, openEndDt):
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
        curPage = 1
        list_exist = True
        while list_exist:
            openEndDt = str(int(openStartDt) + period - 1)
            df, list_exist = self.request_MovieList(curPage, openStartDt, openEndDt)      
            movie_list = pd.concat([movie_list, df], ignore_index=True)
            curPage += 1
        self.save_data(movie_list, f"MovieList_S{openStartDt}_E{openEndDt}")
        return movie_list

    def get_BoxOffice(self, startDt, period):
        extract_range = self.get_extract_range(startDt, period)
        boxoffice_df = pd.DataFrame()
        for extract_date in tqdm(extract_range):
            df = self.request_DailyBoxOffice(extract_date)
            df["targetDt"] = extract_date[:4] + "-" + extract_date[4:6] + "-" + extract_date[6:]
            boxoffice_df = pd.concat([boxoffice_df, df], ignore_index=True)
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
        boxoffice_df = boxoffice_df.astype(col_types)
        boxoffice_df["elapsedDt"] = (boxoffice_df["targetDt"] - boxoffice_df["openDt"]) / np.timedelta64(1, 'D')
        self.save_data(boxoffice_df, f"BoxOffice_S{startDt}_E{extract_date}")
        return boxoffice_df
    

if __name__ == '__main__':
    movie = Movie()
    # df = movie.get_BoxOffice("20231122", 10)
    df = movie.get_MovieList("2021", 2)
    print(df)