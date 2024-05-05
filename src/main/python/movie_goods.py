import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
import re
from fake_useragent import UserAgent

class MovieGoods():
    def get_CGV_GoodsIdx(self, event_idx):
        url = "https://m.cgv.co.kr/Event/GiveawayEventDetail.aspx/GetGiveawayEventDetail"
        body = {
            "eventIndex": event_idx,
            "giveawayIndex": None
        }
        response = requests.post(url, json=body)
        text = response.text
        loads = json.loads(text)
        goods_idx = loads["d"]["GiveawayItemList"][0]["GiveawayItemCode"]
        return goods_idx

    def get_CGV_EventList(self, movie_filter='', goods_filter=''):
        url = "https://m.cgv.co.kr/Event/GiveawayEventList.aspx/GetGiveawayEventList"
        body = {
            "theaterCode": "",
            "pageNumber": "1",
            "pageSize": "100"
        }
        response = requests.post(url, json=body)
        text = response.text
        loads = json.loads(text)
        # soup = BeautifulSoup(loads['d']['List'])
        soup = BeautifulSoup(loads['d']['List'], "html.parser")
        event_list = soup.find_all("li")
        event_df = pd.DataFrame()
        p = re.compile(r'\[([\d\D]+)\]') # 영화이름 추출 정규식
        for event in event_list:
            event_idx = event['data-eventidx']
            goods_idx = self.get_CGV_GoodsIdx(event_idx)
            event_name = event.find("strong").get_text()
            event_period = event.find("span").get_text()
            m = p.match(event_name)
            if m is None:
                movie_name = event_name
                goods_name = event_name
            else:
                movie_name = m.group()[1:-1]
                goods_name = event_name[m.end():] # 띄어쓰기 했다안했다
            event_dict = {"event_idx":event_idx, "goods_idx":goods_idx, "movie":movie_name, "goods":goods_name, "period":event_period}
            event_df = pd.concat([event_df, pd.DataFrame.from_dict(event_dict, orient='index').T], ignore_index=True)
        event_df = event_df[
            (event_df["movie"].str.contains(movie_filter)) &
            (event_df["goods"].str.contains(goods_filter))
        ]
        # 날짜 필터도 추가 예정
        # 위에서 필터링
        return event_df

    def get_CGV_Theaters(self, giveawayIndex):
        url = "https://m.cgv.co.kr/Event/GiveawayEventDetail.aspx/GetGiveawayTheaterInfo"
        body = {
            "giveawayIndex": giveawayIndex,
            "areaCode": None
        }
        response = requests.post(url, json=body)
        text = response.text
        loads = json.loads(text)
        AreaList = loads["d"]["AreaList"]
        Theaters = pd.DataFrame()
        for Area in AreaList:
            AreaCode = Area["AreaCode"]
            body = {
                "giveawayIndex": giveawayIndex,
                "areaCode": AreaCode
            }
            response = requests.post(url, json=body)
            text = response.text
            loads = json.loads(text)
            Theater = pd.DataFrame(loads["d"]["TheaterList"])
            Theaters = pd.concat([Theaters, Theater[["TheaterName", "TheaterCode", "CountTypeCode"]]], ignore_index=True)
        return Theaters

    def Get_CGV_GoodsStatus(self, movie_filter="", goods_filter=""):
        event_df = self.get_CGV_EventList(movie_filter=movie_filter, goods_filter=goods_filter)
        GoodsStatus = pd.DataFrame()
        for idx in event_df.index:
            giveawayIndex = event_df.loc[idx]["goods_idx"]
            Theaters = self.get_CGV_Theaters(giveawayIndex)
            Theaters["movie"] =  event_df.loc[idx]["movie"]
            Theaters["goods"] =  event_df.loc[idx]["goods"]
            Theaters["period"] =  event_df.loc[idx]["period"]
            GoodsStatus = pd.concat([GoodsStatus, Theaters], ignore_index=True)
        return GoodsStatus

    def get_LC_EventList(self):
        url = "https://event.lottecinema.co.kr/LCWS/Event/EventData.aspx"
        # 특별기획전 "EventClassificationCode":"10"
        body = {
            "ParamList": '{"MethodName":"GetEventLists","channelType":"","osType":"","osVersion":"","EventClassificationCode":"20","SearchText":"","CinemaID":"","PageNo":1,"PageSize":100,"MemberNo":"0"}'
        }
        response = requests.post(url, data=body)
        text = response.text
        loads = json.loads(text)
        event_df = pd.DataFrame(loads["Items"])

        goods_info = [self.get_LC_GoodsInfo(x) for x in event_df['EventID']]
        event_df = pd.concat([event_df[["EventID", "EventName", "ProgressStartDate", "ProgressEndDate"]], 
                            pd.json_normalize(goods_info)[["FrGiftID", "FrGiftNm"]]],
                            axis=1)

        event_df = event_df[~event_df['FrGiftID'].isna()].reset_index(drop=True)

        p = re.compile(r'\<([\d\D]+)\>') # 영화이름 추출 정규식
        event_df["movie_name"] = [
            '' if p.match(x) is None else p.match(x).group()[1:-1] 
            for x in event_df['EventName']
        ]

        p = re.compile(r'\,([\d\D]+)\)') # 굿즈이름 추출 정규식
        event_df["goods_name"] = [
            '' if len(p.findall(x))==0 else p.findall(x)[0].replace(' ', '')
            for x in event_df['FrGiftNm']
        ]
        return event_df

    def get_LC_GoodsInfo(self, event_idx):
        # EventID, FrGiftID, FrGiftNm
        url = "https://event.lottecinema.co.kr/LCWS/Event/EventData.aspx"
        body = {
            "ParamList": '{"MethodName":"GetInfomationDeliveryEventDetail","channelType":"","osType":"","osVersion":"","EventID":%s}' % (event_idx)
        }
        response = requests.post(url, data=body)
        text = response.text
        loads = json.loads(text)
        goods_info = loads["InfomationDeliveryEventDetail"][0]["GoodsGiftItems"]
        if len(goods_info) == 0: 
            goods_info = None
        else:
            goods_info = goods_info[0]
        return goods_info

    def get_LC_Theaters(self, EventID, FrGiftID):
        url = "https://event.lottecinema.co.kr/LCWS/Event/EventData.aspx"
        body = {
            "ParamList": '{"MethodName":"GetCinemaGoods","channelType":"","osType":"","osVersion":"","EventID":%s, "GiftID":%s}' % (EventID, FrGiftID)
        }
        response = requests.post(url, data=body)
        text = response.text
        loads = json.loads(text)
        Theaters = pd.DataFrame(loads["CinemaDivisionGoods"])
        return Theaters

    def Get_LC_GoodsStatus(self, movie_filter="", goods_filter=""):
        event_df = self.get_LC_EventList()
        event_df = event_df[
            (event_df["EventName"].str.contains(movie_filter)) &
            (event_df["FrGiftNm"].str.contains(goods_filter))
        ]
        GoodsStatus = pd.DataFrame()
        for idx in event_df.index:
            EventID = event_df.loc[idx]["EventID"]
            FrGiftID = event_df.loc[idx]["FrGiftID"]
            Theaters = self.get_LC_Theaters(EventID, FrGiftID)
            Theaters["movie"] =  event_df.loc[idx]["movie_name"]
            Theaters["goods"] =  event_df.loc[idx]["goods_name"]
            Theaters["period"] =  event_df.loc[idx]["ProgressStartDate"] + '~' + event_df.loc[idx]["ProgressEndDate"]
            GoodsStatus = pd.concat([GoodsStatus, Theaters], ignore_index=True)
        return GoodsStatus

    def get_MB_EventList(self):
        url = "https://www.megabox.co.kr/on/oh/ohe/Event/eventMngDiv.do"
        body = {
            "currentPage": "1",
            "recordCountPerPage": "1000",
            "eventStatCd": "ONG",
            "eventTitle": "",
            "eventDivCd": "CED03",
            "eventTyCd": "",
            "orderReqCd": "ONGlist"
        }
        ua = UserAgent()
        headers = {
            'User-Agent': ua.random
        }
        response = requests.post(url, json=body, headers=headers)
        text = response.text
        soup = BeautifulSoup(text, features="html.parser")

        eventBtn_list = soup.find_all("a", class_="eventBtn")
        event_list = [
            { 
                "event_no": x["data-no"],
                "event_name": x["title"],
                "period": x.find("p", class_="date").get_text().replace("\t", "").replace("\n", "")
            }
            for x in eventBtn_list
        ]
        event_df = pd.DataFrame(event_list)
        goods_info = [self.get_MB_GoodsInfo(x) for x in event_df['event_no']]
        event_df = pd.concat([event_df, pd.json_normalize(goods_info)], axis=1)
        return event_df


    def get_MB_GoodsInfo(self, event_idx):
        url = f"https://www.megabox.co.kr/event/detail?eventNo={event_idx}"
        ua = UserAgent()
        headers = {
            'User-Agent': ua.random
        }
        response = requests.post(url, headers=headers)
        text = response.text
        soup = BeautifulSoup(text, features="html.parser")
        goods_tag = soup.find("button", attrs={"id":"btnSelectGoodsStock"})
        if goods_tag is None:
            goods_info = None
        else:
            goods_info = {
                "data-nm": goods_tag["data-nm"],
                "data-pn": goods_tag["data-pn"]
            }
        return goods_info

    def get_MB_Theaters(self, goodsNo):
        url = "https://www.megabox.co.kr/on/oh/ohe/Event/selectGoodsStockPrco.do"
        body = {
            "goodsNo": goodsNo
        }
        ua = UserAgent()
        headers = {
            'User-Agent': ua.random
        }
        response = requests.post(url, data=body, headers=headers)
        text = response.text
        soup = BeautifulSoup(text, features="html.parser")
        brch_list = soup.find_all("li", class_="brch")
        theater_list = [ 
            { 
                "theater": x.find("a").get_text(),
                "status": x.find("span").get_text()        
            }
            for x in brch_list
        ]
        Theaters = pd.DataFrame(theater_list)
        return Theaters

    def Get_MB_GoodsStatus(self, goods_filter=""):
        event_df = self.get_MB_EventList()
        event_df = event_df[~event_df['data-pn'].isna()].sort_values('data-pn')
        event_df = event_df[
            (event_df["data-nm"].str.contains(goods_filter))
        ]
        GoodsStatus = pd.DataFrame()
        for idx in event_df.index:
            goodsNo = event_df.loc[idx]["data-pn"]
            Theaters = self.get_MB_Theaters(goodsNo)
            Theaters["event_name"] =  event_df.loc[idx]["event_name"]
            Theaters["data-nm"] =  event_df.loc[idx]["data-nm"]
            Theaters["period"] =  event_df.loc[idx]["period"]
            GoodsStatus = pd.concat([GoodsStatus, Theaters], ignore_index=True)
        return GoodsStatus

if __name__ == '__main__':
    movie_goods = MovieGoods()
    # goods = movie_goods.Get_CGV_GoodsStatus(movie_filter='범죄도시')
    # goods = movie_goods.Get_LC_GoodsStatus(movie_filter='범죄도시')
    goods = movie_goods.Get_MB_GoodsStatus(goods_filter='범죄도시')
    print(goods)
