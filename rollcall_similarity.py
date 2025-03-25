import torch
import pandas as pd
import os
from transformers import pipeline
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm

os.environ["TOKENIZERS_PARALLELISM"] = "false"

summarizer = pipeline("summarization", model="knkarthick/MEETING_SUMMARY", device=0)
sentencoModel = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2', device='cuda')

df_rollcall = pd.read_csv('data/rollcall_script.csv')
df_available = pd.read_csv('data/available.csv')
df_closed_trump = pd.read_csv('closed_trump_questions_description.csv')

df_merged = pd.merge(df_available, df_closed_trump, on='condition_id', how='inner')

df_merged['start_date'] = pd.to_datetime(df_merged['start_date'])
df_merged['end_date_x'] = pd.to_datetime(df_merged['end_date_x'])

matching_list = [] # 매칭된 질문 리스트

grp_df_rollcall = df_rollcall.groupby('Title') # column = [Title,Speaker,Timestamp,Content,Video Link,date]

# Concatenate each row like 'Speaker: Content. Speaker: Content ...'
df_rollcall_concat = grp_df_rollcall.apply(
    lambda x: pd.Series({
        'Concatenated_Content': '\n'.join(f"{row['Speaker']}: {row['Content']}" for _, row in x.iterrows()),
        'date': x['date'].iloc[0],
        'url': x['Video Link'].iloc[0]
    })
).reset_index()

df_rollcall_concat.columns = ['Title', 'Concatenated_Content', 'date','url']

for index, row in tqdm(df_rollcall_concat.iterrows(), total=df_rollcall_concat.shape[0]):
    title = row['Title']
    url = row['url']
    concatenated_content = row['Concatenated_Content']
    upload_date = pd.to_datetime(df_rollcall_concat['date'][index]).tz_localize('UTC')
    filtered_questions = df_merged[(df_merged['start_date'] <= upload_date) & (df_merged['end_date_x'] >= upload_date)]['question'].tolist()

    if len(filtered_questions) == 0:
        continue

    summary = summarizer(concatenated_content, max_length=1024, min_length=30, do_sample=False,truncation=True)
    summ_text = summary[0]['summary_text']

    summ_emb = sentencoModel.encode(summ_text)

    similarity_list = []

    for question in filtered_questions:
        question_embedding = sentencoModel.encode(question)
        similarity = cosine_similarity([summ_emb], [question_embedding])
        similarity_list.append({'question':question,'similarity': similarity[0][0]})

    df_similarity = pd.DataFrame(similarity_list)
    df_similarity = df_similarity.sort_values(by='similarity',ascending=False)

    matching_questions = df_similarity[df_similarity['similarity'] > 0.4][['question', 'similarity']].to_dict('records')
    for matching_question in matching_questions:
        matching_list.append({'title': title, 'url': url, 'matching_questions': matching_question['question'], 'similarity': matching_question['similarity']})

    df_matching = pd.DataFrame(matching_list, columns=['title', 'url', 'matching_questions', 'similarity'])
    df_matching.to_csv('matching_questions_rollcall.csv', index=False)