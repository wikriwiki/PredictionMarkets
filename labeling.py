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
from clob_client import PolymarketClient,timestamp_to_datetime