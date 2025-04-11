from clob_client import PolymarketClient,timestamp_to_datetime
import pandas as pd
from tqdm import tqdm

question_dir = "closed_question_politics_economy.csv"
price_dir = "data/price"
df_questions = pd.read_csv(question_dir)
id_list = df_questions['condition_id'].tolist()
client = PolymarketClient()

available_list = []

for id in tqdm(id_list):
    try:
        price_data = client.get_price(id,"Yes")['history']
        data_list = []
        for i in price_data:
            datetime_stamp = timestamp_to_datetime(i['t'])
            data_list.append({'datetime': datetime_stamp, 'price': i['p']})
        df_price = pd.DataFrame(data_list, columns=['datetime', 'price'])
        # df_price.to_csv(f"{price_dir}/{id}.csv",index=False)
        available_list.append({'condition_id':id,'start_date':df_price['datetime'].tolist()[0],'end_date':df_price['datetime'].tolist()[-1]})
    except:
        pass

df_available = pd.DataFrame(available_list,columns=['condition_id','start_date','end_date'])
print(df_available)
df_available.to_csv("data/available_politics_economy.csv",index=False)