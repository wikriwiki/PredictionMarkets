import pandas as pd

df_closed = pd.read_csv('closed_questions_politics_economy.csv').loc[:,['question','condition_id','end_date']]
df_cnn = pd.read_csv("matching_questions_politics_economy.csv")
df_fox = pd.read_csv("matching_questions_politics_economy_fox.csv")

df_cnn_merged = pd.merge(df_closed, df_cnn, left_on='question', right_on='matching_questions', how='inner')
df_fox_merged = pd.merge(df_closed, df_fox, left_on='question', right_on='matching_questions', how='inner')

# drop end_date < upload_date
df_cnn_merged.drop(df_cnn_merged[df_cnn_merged['end_date'] <= df_cnn_merged['upload_date']].index, inplace=True)

print(df_closed.shape)
print(df_cnn.shape)
print(df_cnn_merged.shape)

print(df_cnn_merged.columns)