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
from datetime import datetime

def utc_to_timestamp(utc_time):
    # UTC 문자열을 datetime 객체로 변환
    utc_datetime = datetime.strptime(utc_time, "%Y-%m-%d %H:%M:%S")
    # datetime 객체를 Unix timestamp로 변환
    timestamp = int(utc_datetime.timestamp())
    return timestamp

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

    def get_market_data(self, condition_id):
        resp = self.client.get_market(condition_id)
        return resp

    def get_price(self, condition_id, outcome):
        resp = self.client.get_market(condition_id)
        token_id = ""
        print(resp['tokens'])
        for resp_token in resp['tokens']:
            if resp_token['outcome'] == outcome:
                token_id = resp_token['token_id']
                break
        print(token_id)

        url = f"{self.client.host}/prices-history"
        
        params={
            "market":token_id,
            "startTs":utc_to_timestamp(resp['start_date']),
            "interval":"1d",
            "fidelity": 60
        }
        resp = requests.get(url, params=params)

        if resp.status_code == 200:
            data = resp.json()
            print(json.dumps(data,indent=4))
        else:
            print(f"Error {resp.status_code}: {resp.text}")

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
    client.save_all_closed_questions()
    # print(json.dumps(client.get_market_data("0xa07f15fdd0baa90adf78bd4d66543083334a7e5235a95fa5f241c4e519ea83f6"), indent=4))
    # client.get_price("0x98d9781facbf448a67bd1e1e0d538b2afca6e538d73f278308d0211fbfc87c94","Yes")