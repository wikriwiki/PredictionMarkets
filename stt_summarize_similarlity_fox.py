import torch
from transformers import pipeline
import whisper
import os
import pandas as pd
import subprocess
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm

os.environ["TOKENIZERS_PARALLELISM"] = "false"

def download_video(url, output_path):

    # 경로 생성
    os.makedirs(output_path, exist_ok=True)

    

    
    command = f'yt-dlp --get-duration --cookies-from-browser firefox {url}'
    result = subprocess.run(command, shell=True, capture_output=True, text=True, check=True)
    duration = result.stdout.strip()

    time_parts = duration.split(':')
    if len(time_parts) == 3:
        hours, minutes, seconds = map(int, time_parts)
    elif len(time_parts) == 2:
        hours = 0
        minutes, seconds = map(int, time_parts)
    elif len(time_parts) == 1:
        hours = 0
        minutes = 0
        seconds = int(time_parts[0])
    else:
        raise ValueError("Unexpected duration format")
    total_seconds = hours * 3600 + minutes * 60 + seconds

    if total_seconds > 3600:
        return total_seconds
    
    else:
       
        try:
            command = f'yt-dlp --force-overwrites -f "bestvideo[height=720]+bestaudio" --cookies-from-browser firefox -o "{output_path}/video2" {url}'
            subprocess.run(command, shell=True, check=True)

            return total_seconds
        except:
            return 3601

   
def speech_to_text(model, audio_path):
    result = model.transcribe(audio_path)
    return result["text"]

summarizer = pipeline("summarization", model="knkarthick/MEETING_SUMMARY", device=0)
sentencoModel = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2', device='cuda')
whisperModel = whisper.load_model("base")

df_youtube = pd.read_csv('data/fox.csv')
df_available = pd.read_csv('data/available_politics_economy.csv')
df_closed_trump = pd.read_csv('closed_questions_politics_economy.csv')

df_merged = pd.merge(df_available, df_closed_trump, on='condition_id', how='inner')

df_merged['start_date'] = pd.to_datetime(df_merged['start_date'])
df_merged['end_date_x'] = pd.to_datetime(df_merged['end_date_x'])

output_path = "content/video_data/"

matching_list = [] 

for i in tqdm(range(3401,len(df_youtube))):
    yt_url = df_youtube['video_link'][i]
    title = df_youtube['title'][i]
    upload_date = pd.to_datetime(df_youtube['upload_date'][i])
    filtered_questions = df_merged[(df_merged['start_date'] <= upload_date) & (df_merged['end_date_x'] >= upload_date)]['question'].tolist()

    if len(filtered_questions) == 0:
        continue

    print("Downloading video...")
    times = download_video(yt_url, output_path)

    if times > 3600:
        continue

    print("STT...")

    text = speech_to_text(whisperModel,f"{output_path}/video2.webm")

    print("Summarize...")
    summary = summarizer(text, max_length=1024, min_length=30, do_sample=False,truncation=True)
    summ_text = summary[0]['summary_text']

    summ_emb = sentencoModel.encode(summ_text)

    similarity_list = []

    print(f"Title: {title}")
    for question in filtered_questions:
        sentence_emb = sentencoModel.encode(question)
        similarity = cosine_similarity([summ_emb],[sentence_emb])
        similarity_list.append({'question':question,'similarity': similarity[0][0]})

    df_similarity = pd.DataFrame(similarity_list)
    df_similarity = df_similarity.sort_values(by='similarity',ascending=False)

    matching_questions = df_similarity[df_similarity['similarity'] >= 0.6][['question', 'similarity']].to_dict('records')
    for matching_question in matching_questions:
        matching_list.append({'title': title, 'url': yt_url, 'upload_date': upload_date,'matching_questions': matching_question['question'], 'similarity': matching_question['similarity'], "text":text})
    df_matching = pd.DataFrame(matching_list,columns=['title','url','upload_date','matching_questions','similarity','text'])
    df_matching.to_csv('matching_questions_politics_economy_fox.csv',index=False)

