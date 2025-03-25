import requests
import json

key = "AIzaSyDrk61IqBAY1sXYMJ4D6H4ci7Pill8_RM0"
q = 'John Hickenlooper Lori Chavez-DeRemer Secretary of Labor?'
url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&maxResults=10&q={q}&safeSearch=none&publishedBefore=2025-03-11T00:00:00Z&videoEmbeddable=any&order=viewCount&type=video&key=" + key

response = requests.get(url)
print(json.dumps(response.json(),indent=4))