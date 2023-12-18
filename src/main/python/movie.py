import requests
import json
import pandas as pd
import numpy as np
import datetime
from tqdm import tqdm
from config import KobisConfig

kobis_config = KobisConfig()

class Movie():
    def __init__(self):
        self.key = kobis_config.key

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
        return boxoffice_df
    

if __name__ == '__main__':
    movie = Movie()
    df = movie.get_BoxOffice("20231122", 10)
    df
    print(df)