import requests
import json
from clob_client import PolymarketClient,timestamp_to_datetime
import pandas as pd
from tqdm import tqdm
from datetime import datetime,timedelta
import spacy
from time import sleep

nlp = spacy.load("en_core_web_sm")

key = "AIzaSyDrk61IqBAY1sXYMJ4D6H4ci7Pill8_RM0"

youtube_url = "https://www.youtube.com/watch?v="

df_questions = pd.read_csv('closed_trump_questions_description.csv',index_col=[0])
df_available = pd.read_csv('data/available.csv')

test_id = ['0xa0ff9815e784f89efe90cd1dac7fe63339180bc4baa0372ec11474a1e84e10a4','0x66a86bed2460bb982e1a9a22d111b08bea41349423ef9e8669570e488cf96de0','0xc18a76cbd702e37c482ef65ab236a6c8ad9126e8de0e3807ee8da8057e03987f']

window_size = timedelta(days=5)

video_list = []

for i in tqdm(range(len(df_available))):
    id = df_available['condition_id'][i]
    if id not in test_id:
        continue
    start_date = datetime.strptime(df_available['start_date'][i],"%Y-%m-%d %H:%M:%S%z")
    end_date = datetime.strptime(df_available['end_date'][i],"%Y-%m-%d %H:%M:%S%z")
    question = df_questions.loc[id]['question']
    question = question.replace('[Single Market]','')
    doc = nlp(question)

    # NER 기반 특정 개체 추출 (PERSON 및 직책 관련)
    entities = [ent.text for ent in doc.ents if ent.label_ in ("PERSON", "NORP", "FAC","GPE","LOC","PRODUCT","EVENT","WORK_OF_ART","ORG", "TITLE")]

    query = ""
    for entity in entities:
        query += entity + " "

    curr_date = start_date + window_size
    prev_date = start_date

    print(end_date-start_date)

    while(curr_date < end_date):
        str_curr_date = curr_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        str_prev_date = prev_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&maxResults=3&q={query} -news&publishedBefore={str_curr_date}&publishedAfter={str_prev_date}&type=video&key=" + key

        response = requests.get(url)
        print(json.dumps(response.json(),indent=4))
        for item in response.json()['items']:
            video_list.append({'question':question,'start_date':prev_date,'end_date':curr_date,'video_title':item['snippet']['title'],'video_url':youtube_url+item['id']['videoId']})

        prev_date = curr_date
        curr_date += window_size

        sleep(0.5)

df_video = pd.DataFrame(video_list,columns=['question','start_date','end_date','video_title','video_url'])
df_video.to_csv('data/video_wo_news.csv',index=False)