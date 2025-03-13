import os
from dotenv import load_dotenv
from py_clob_client.constants import POLYGON
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs
from py_clob_client.order_builder.constants import BUY
import json
import time
import pandas as pd

load_dotenv(override=True)

print(os.getenv("PK"))

host = "https://clob.polymarket.com"
key = os.getenv("PK")
chain_id = POLYGON

# Create CLOB client and get/set API credentials
client = ClobClient(host, key=key, chain_id=chain_id)
client.set_api_creds(client.create_or_derive_api_creds())

next_cursor = ""

df_qd = pd.DataFrame(columns=['condition_id', 'question', 'description', 'end_date'])

while next_cursor != "LTE=":
    time.sleep(0.5)
    resp = client.get_sampling_markets(next_cursor = next_cursor)
    next_cursor = resp['next_cursor']
    # print(resp['data'][0]['question'])
    for resp_data in resp['data']:
        len_df = len(df_qd)
        replaced_description = resp_data['description'].replace('\"','"')
        df_qd.loc[len_df] = [resp_data['condition_id'], resp_data['question'], replaced_description, resp_data['end_date_iso']]


    # for resp_data in resp['data']:
    #     # json_resp = json.dumps(resp_data, indent=4)
    #     print(resp_data['question'])
df_qd.to_csv('questions_description.csv', index=False)
print("Done!")

# # Create and sign an order buying 100 YES tokens for 0.50c each
# resp = client.create_and_post_order(OrderArgs(
#     price=0.50,
#     size=100.0,
#     side=BUY,
#     token_id="71321045679252212594626385532706912750332728571942532289631379312455583992563"
# ))

# print(resp)