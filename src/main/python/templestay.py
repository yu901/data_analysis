import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

base_url = "https://www.templestay.com"
list_url = f"{base_url}/fe/MI000000000000000072/templestay/recList.do"
detail_base_url = f"{base_url}/fe/MI000000000000000072/reserve/view.do?templestaySeq="

# 결과 저장용 리스트
results = []
seen_seqs = set() 

# 페이지 순회 (현재 1~7 페이지 존재)
for page in range(1, 8):
    print(f"📄 페이지 {page} 처리 중...")

    # 페이지 요청
    params = {"pageIndex": page}
    response = requests.get(list_url, params=params)
    soup = BeautifulSoup(response.text, "html.parser")

    # 예약하기 버튼에서 templestaySeq 추출
    for tag in soup.select("a[href^=javascript\\:fncReserve]"):
        onclick = tag.get("href")
        if "fncReserve" in onclick:
            seq = onclick.split("'")[1]
            if seq in seen_seqs:
                continue
            seen_seqs.add(seq)

            detail_url = detail_base_url + seq
            detail_resp = requests.get(detail_url)
            detail_soup = BeautifulSoup(detail_resp.text, "html.parser")

            info = {
                "templestaySeq": seq,
                "담당자": None,
                "이메일": None,
                "주소": None,
                "연락처": None,
                "예약현황": {}
            }

            # 담당자, 이메일, 주소, 연락처 수집
            info_ul = detail_soup.select_one("div.info > ul")
            if info_ul:
                for li in info_ul.find_all("li"):
                    text = li.get_text(strip=True)
                    if "담당자" in text:
                        info["담당자"] = text.split(":", 1)[-1].strip()
                    elif "@" in text:
                        info["이메일"] = text
                    elif "," in text:
                        info["주소"] = text
                    elif "-" in text:
                        info["연락처"] = text

            # 예약 가능 날짜 수집
            calendar = detail_soup.select("table tbody#nowCalArea td")
            reserve_info = {}

            for td in calendar:
                date_id = td.get("id")  # format: 20250417
                td_class = td.get("class", [])

                if not date_id:
                    continue

                day = int(date_id[-2:])
                month = int(date_id[4:6])

                if "day" in td_class:
                    if "disable" not in td_class:
                        reserve_info[f"{month}/{day}"] = "가능"
                    elif "fullBooking_fe" in td_class:
                        reserve_info[f"{month}/{day}"] = "불가"

            info["예약현황"] = reserve_info
            results.append(info)

            # ✅ "가능"한 날짜가 하나라도 있는 경우만 출력
            if "가능" in reserve_info.values():
                print(f"✅ 예약 가능: {info['주소']}, 예약 현황: {info['예약현황']}, URL: {detail_url}")
            time.sleep(0.2)  # 너무 빠르게 요청하지 않도록 sleep

# 데이터프레임 생성
df = pd.DataFrame(results)

# CSV 저장
df.to_csv("templestay_details.csv", index=False, encoding="utf-8-sig")

# 출력
print("✅ 수집 완료. 'templestay_details.csv' 파일로 저장되었습니다.")