import os
from dotenv import load_dotenv
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

class PolymarketGammaClient:
    def __init__(self):
        load_dotenv(override=True)

        self.host = 'https://gamma-api.polymarket.com'
        self.key = os.getenv("PK")

    def get_
    