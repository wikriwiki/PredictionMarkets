import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta

today = datetime.today()

def convert_relative_date(relative_date):
    match = re.search(r"(\d+) (hour|day|week|month|year)s? ago", relative_date)
    if match:
        value, unit = int(match.group(1)), match.group(2)
        if unit == "hour":
            date = today - timedelta(hours=value)
        elif unit == "day":
            date = today - timedelta(days=value)
        elif unit == "week":
            date = today - timedelta(weeks=value)
        elif unit == "month":
            date = today - timedelta(days=value * 30)  # 대략적인 변환
        elif unit == "year":
            date = today - timedelta(days=value * 365)  # 대략적인 변환
        return date.strftime("%Y-%m-%d")
    return "Unknown"

#  유튜브 채널 URL 입력
CHANNEL_URL = "https://www.youtube.com/@CNN/videos"

#  Firefox WebDriver 설정 (Linux용)
firefox_options = Options()
# firefox_options.add_argument("--headless")  # GUI 없이 실행 (백그라운드 모드)
firefox_options.add_argument("--disable-gpu")

service = Service("/usr/local/bin/geckodriver")  # GeckoDriver 경로 지정
driver = webdriver.Firefox(service=service, options=firefox_options)

# 유튜브 채널 접속
driver.get(CHANNEL_URL)
time.sleep(5)  # 페이지 로딩 대기

# 스크롤을 한 번만 내리기
driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
time.sleep(3)

# BeautifulSoup으로 HTML 파싱
soup = BeautifulSoup(driver.page_source, "html.parser")
driver.quit()  #  크롬 드라이버 종료

#  동영상 제목, 링크, 조회수 수집
videos = []
for video in soup.find_all("a", id="video-title-link"):
    title = video.get("title")
    link = "https://www.youtube.com" + video.get("href")
    
    # 조회수 추출 (조회수 텍스트가 있는 형제 요소 찾기)
    # parent_div = video.find_parent("ytd-rich-grid-media")
    # if parent_div:
    views_text = video.get('aria-label')
    match = re.search(r"(\d[\d,]*) views", views_text)
    if match:
        views = int(match.group(1).replace(",", ""))

        date_match = re.search(r"(\d+ (?:hour|day|week|month|year)s? ago)", views_text)
        relative_date = date_match.group(1) if date_match else "Unknown"
        upload_date = convert_relative_date(relative_date)

        if views >= 100000:  #  100K 이상 필터링
            videos.append({"Title": title, "Views": views, "Upload Date": upload_date, "URL": link})

#  결과 저장 (CSV 파일)
df = pd.DataFrame(videos)
df.to_csv("youtube_videos_over_100k.csv", index=False)
