import os
from dotenv import load_dotenv
from py_clob_client.constants import POLYGON
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs
from py_clob_client.order_builder.constants import BUY
import json
import requests
import time
import pandas as pd
from datetime import datetime,timezone

def utc_to_timestamp(utc_time):
    # UTC 문자열을 datetime 객체로 변환
    utc_datetime = datetime.strptime(utc_time, "%Y-%m-%dT%H:%M:%SZ")
    # datetime 객체를 Unix timestamp로 변환
    timestamp = int(utc_datetime.timestamp())
    return timestamp

def timestamp_to_datetime(timestamp):
    # Unix timestamp를 datetime 객체로 변환 (UTC 기준)
    return datetime.fromtimestamp(timestamp,timezone.utc)

class PolymarketClient:
    def __init__(self):
        load_dotenv(override=True)

        host = "https://clob.polymarket.com"
        key = os.getenv("PK")
        chain_id = POLYGON

        # Create CLOB client and get/set API credentials
        self.client = ClobClient(host, key=key, chain_id=chain_id)
        self.client.set_api_creds(self.client.create_or_derive_api_creds())

    def save_all_questions(self):
        next_cursor = ""
        df_qd = pd.DataFrame(columns=['condition_id', 'question', 'description', 'end_date'])
        while next_cursor != "LTE=":
            time.sleep(0.5)
            resp = self.client.get_sampling_markets(next_cursor = next_cursor)
            next_cursor = resp['next_cursor']
            for resp_data in resp['data']:
                if "Politics" in resp_data["tags"]:
                    len_df = len(df_qd)
                    df_qd.loc[len_df] = [resp_data['condition_id'], resp_data['question'], resp_data['description'], resp_data['end_date_iso']]
        df_qd.to_csv('questions_description.csv', index=False)
        print("Done!")

    def save_all_closed_questions(self):
        next_cursor = ""
        df_qd = pd.DataFrame(columns=['condition_id', 'question', 'description', 'end_date'])
        while next_cursor != "LTE=":
            time.sleep(0.5)
            resp = self.client.get_markets(next_cursor = next_cursor)
            next_cursor = resp['next_cursor']
            for resp_data in resp['data']:
                try:
                    if "Politics" in resp_data["tags"] and resp_data['closed']:
                        len_df = len(df_qd)
                        df_qd.loc[len_df] = [resp_data['condition_id'], resp_data['question'], resp_data['description'], resp_data['end_date_iso']]
                except:
                    pass
        df_qd.to_csv('closed_questions_description.csv', index=False)
        print("Done!")

    def save_all_closed_trump_questions(self):
        next_cursor = ""
        df_qd = pd.DataFrame(columns=['condition_id', 'question', 'description', 'end_date'])
        while next_cursor != "LTE=":
            time.sleep(0.5)
            resp = self.client.get_markets(next_cursor = next_cursor)
            next_cursor = resp['next_cursor']
            for resp_data in resp['data']:
                try:
                    if "Trump" in resp_data["tags"] and resp_data['closed']:
                        len_df = len(df_qd)
                        df_qd.loc[len_df] = [resp_data['condition_id'], resp_data['question'], resp_data['description'], resp_data['end_date_iso']]
                except:
                    pass
        df_qd.to_csv('closed_trump_questions_description.csv', index=False)
        print("Done!")

    def get_market_data(self, condition_id):
        resp = self.client.get_market(condition_id)
        return resp

    def get_price(self, condition_id, outcome):
        resp = self.client.get_market(condition_id)
        token_id = ""
        for resp_token in resp['tokens']:
            if resp_token['outcome'] == outcome:
                token_id = resp_token['token_id']
                break

        url = f"{self.client.host}/prices-history"
        
        params={
            "market":token_id,
            "startTs":0,
            "endTs":int(datetime.now().timestamp()),
            "interval":"1d",
            "fidelity": 1440
        }
        resp = requests.get(url, params=params)

        if resp.status_code == 200:
            data = resp.json()
            # print(json.dumps(data,indent=4))
            return data
        else:
            print(f"Error {resp.status_code}: {resp.text}")
            
    def get_price_at_date(self, condition_id, outcome, target_date):
        resp = self.client.get_market(condition_id)
        token_id = ""
        for resp_token in resp['tokens']:
            if resp_token['outcome'] == outcome:
                token_id = resp_token['token_id']
                break

        if not token_id:
            return None

        url = f"{self.client.host}/prices-history"
        target_ts = utc_to_timestamp(target_date)
        
        params = {
            "market": token_id,
            "startTs": target_ts - 86400,  # 하루 전부터
            "endTs": target_ts + 86400,    # 하루 후까지
            "interval": "1h",
            "fidelity": 60
        }
        
        resp = requests.get(url, params=params)
        if resp.status_code == 200:
            data = resp.json()['history']
            # 가장 가까운 시간대의 가격 찾기
            closest_price = None
            min_time_diff = float('inf')
            
            for price_point in data:
                time_diff = abs(price_point['t'] - target_ts)
                if time_diff < min_time_diff:
                    min_time_diff = time_diff
                    closest_price = price_point['p']
            
            return closest_price
        return None
# # Create and sign an order buying 100 YES tokens for 0.50c each
# resp = client.create_and_post_order(OrderArgs(
#     price=0.50,
#     size=100.0,
#     side=BUY,
#     token_id="71321045679252212594626385532706912750332728571942532289631379312455583992563"
# ))

# print(resp)

if __name__ == "__main__":
    client = PolymarketClient()
    # client.save_all_questions()
    # client.save_all_questions()
    # client.save_all_closed_trump_questions()
    # print(json.dumps(client.get_market_data("0x9ea20241be7a9bd20668563386a4d53dbd3ad5b05d16d5ac5264ebdbdeb3b0a0"), indent=4))
    # price_data = client.get_price("0x1f5e91cec5de2c58c8b51e5830819bbed6c0978dc05b964e4f432d7744977d2a","Yes")['history']
    # data_list = []
    # for i in price_data:
    #     datetime_stamp = timestamp_to_datetime(i['t'])
    #     print(datetime_stamp)
    #     data_list.append({'datetime': datetime_stamp, 'price': i['p']})
    # df_price = pd.DataFrame(data_list, columns=['datetime', 'price'])
    # df_price.to_csv("price.csv",index=False)
    gamma = 'https://gamma-api.polymarket.com'
    key = os.getenv("PK")
    
    q = gamma + "/events?limit=3&closed=true&volume_min=100000"

    resp = requests.get(q, headers={"Authorization": key})
    print(json.dumps(resp.json(), indent=4))

