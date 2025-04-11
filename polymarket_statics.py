import pandas as pd

# CSV 파일을 불러온다고 가정하거나 이미 DataFrame이 있다면 바로 사용
df = pd.read_csv("closed_question_politics_economy.csv")

# end_date를 datetime 형식으로 변환
df['end_date'] = pd.to_datetime(df['end_date'])

# 연도만 추출
df['year'] = df['end_date'].dt.year

# 연도별 샘플 수 세기
year_counts = df['year'].value_counts().sort_index()

# 출력
print(year_counts)
