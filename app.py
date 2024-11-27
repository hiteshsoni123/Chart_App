# import struct
# import json
# from fastapi import FastAPI, WebSocket, WebSocketDisconnect
# from fastapi.middleware.cors import CORSMiddleware
# import asyncio
# import websockets
# import aiohttp
# import requests
# import pandas as pd
# from cachetools import cached, TTLCache
# import http.client
# import datetime
# from datetime import datetime, timedelta
# import time
# from collections import defaultdict



# app = FastAPI()

# # Add CORS Middleware for WebSocket compatibility
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],  # Allow all origins for testing; restrict in production
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# URL = "wss://smartapisocket.angelone.in/smart-stream?clientCode=A537412&feedToken=eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6IkE1Mzc0MTIiLCJpYXQiOjE3MzIzMzg5NjksImV4cCI6MTczMjQyNTM2OX0.eFl4ZKLDZ3_vZRx4DySTLjxs7KSkYf0cr8XwuM8n38LAC07JcM2YJ6YB-If6uR1lMddWFTTin_3o-cLDjSiZwQ&apiKey=9H7XvPCk"
        
# HEADERS = {
#     'Authorization': 'Bearer eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6IkE1Mzc0MTIiLCJyb2xlcyI6MCwidXNlcnR5cGUiOiJVU0VSIiwidG9rZW4iOiJleUpoYkdjaU9pSlNVekkxTmlJc0luUjVjQ0k2SWtwWFZDSjkuZXlKMWMyVnlYM1I1Y0dVaU9pSmpiR2xsYm5RaUxDSjBiMnRsYmw5MGVYQmxJam9pZEhKaFpHVmZZV05qWlhOelgzUnZhMlZ1SWl3aVoyMWZhV1FpT2pFeExDSnpiM1Z5WTJVaU9pSXpJaXdpWkdWMmFXTmxYMmxrSWpvaVlqWmhPVGRqWVdNdE4yUmlPUzB6WVdVeExUZzNaV1F0Wm1KbU9ERmtNV1JpTVRBNElpd2lhMmxrSWpvaWRISmhaR1ZmYTJWNVgzWXhJaXdpYjIxdVpXMWhibUZuWlhKcFpDSTZNVEVzSW5CeWIyUjFZM1J6SWpwN0ltUmxiV0YwSWpwN0luTjBZWFIxY3lJNkltRmpkR2wyWlNKOWZTd2lhWE56SWpvaWRISmhaR1ZmYkc5bmFXNWZjMlZ5ZG1salpTSXNJbk4xWWlJNklrRTFNemMwTVRJaUxDSmxlSEFpT2pFM016STBNalV6Tmprc0ltNWlaaUk2TVRjek1qTXpPRGM0T1N3aWFXRjBJam94TnpNeU16TTROemc1TENKcWRHa2lPaUkwTXpBeFpEbGtaQzFqT0RnekxUUmlZMkV0WWpJd015MWxObVZrTjJJeFl6TXpZelVpTENKVWIydGxiaUk2SWlKOS5ZRjV3YlVKQnBKcVRYbFA5ZGFYZElienQwRm9qSjVnOFk2MHZlR3d4eV9ZZllxakVPeDNBVXNOWU03VVZrVUtKXzVGRUMwdzB4eVQ5MERHVmNBOHlQRFpzTnlKdE4ycGxYOVNiUHRuOEdIOFY4d056VVNTSGFGZEtmbzFzSldNckNPQldoc3BGOUlmdjU3a1BVZGVBeTgyVGFWSWo4cGU5NUtfQWZTTGY2QlkiLCJBUEktS0VZIjoiOUg3WHZQQ2siLCJpYXQiOjE3MzIzMzg5NjksImV4cCI6MTczMjQyNTM2OX0._uhTQbOFIFjtw_GxRrake54dTIOrQS6aklPtIAhp88qK0SfNBzsoeW1Zkfbvrs5SHJdf3n5rzL6R4vBKe1FXWQ',
#     "x-api-key": "9H7XvPCk",
#     "x-feed-token": "eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6IkE1Mzc0MTIiLCJpYXQiOjE3MzIzMzg5NjksImV4cCI6MTczMjQyNTM2OX0.eFl4ZKLDZ3_vZRx4DySTLjxs7KSkYf0cr8XwuM8n38LAC07JcM2YJ6YB-If6uR1lMddWFTTin_3o-cLDjSiZwQ"
#         }

# EXCHANGE_TYPE_MAP = {
#     "NSE": 1,
#     "NFO": 2,
#     "BSE": 3,
#     "BFO": 4,
#     "MCX": 5,

#     "CDS": 7,
#     "NCO": 13,
#     "NCDEX": 11
# }

# # Utility Functions
# def unpack_data(binary_data, start, end, byte_format="q"):
#     """Unpacks a slice of binary data using the specified format."""
#     relevant_bytes = binary_data[start:end]
#     return struct.unpack(byte_format, relevant_bytes)[0]

# def _parse_token_value(binary_packet):
#     """Extracts the token from binary data."""
#     token = ""
#     for byte in binary_packet:
#         char = chr(byte)
#         if char == "\x00":
#             return token
#         token += char
#     return token


# # Cache for storing fetched data with a Time-To-Live (TTL) of 300 seconds (5 minutes)
# cache = TTLCache(maxsize=1, ttl=300)
# @cached(cache)
# def fetch_data():
#     api_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
#     try:
#         headers = {"Accept-Encoding": "gzip"}  # Request compressed data
#         response = requests.get(api_url, headers=headers)
#         response.raise_for_status()
#         data = response.json()
#         print("Fetched new data from API")  # For debugging to see when the API is called
#         return pd.DataFrame(data)
#     except requests.exceptions.RequestException as e:
#         print(f"An error occurred: {e}")
#         return None



# # -------------------------- OPEN WATCHLIST -----------------------------------------

# # Parsing Functions
# def parse_watchlist_response(binary_data):
#     try:
#         token = _parse_token_value(binary_data[2:27])
#         last_traded_price = unpack_data(binary_data, 43, 51, byte_format="q") / 100.0
#         closed_price = unpack_data(binary_data, 115, 123, byte_format="q") / 100.0

#         if closed_price != 0:
#             percent_change = round(((last_traded_price - closed_price) / closed_price) * 100, 2)
#         else:
#             percent_change = None

#         df = fetch_data()
#         if df is not None:
#             symbol_df = df[df['token'].isin([token])][['symbol']]
#             if not symbol_df.empty:
#                 symbol = symbol_df.iloc[0]['symbol']  
#                 print("exch:", symbol)


#         return {
#             "type": "watchlist",
#             "token": token,
#             "symbol": symbol,
#             "ltp": last_traded_price,
#             "percent_change": percent_change,
#         }
#     except Exception as e:
#         print(f"Error parsing watchlist response: {e}")
#         return None
    
# # -------------------------- CLOSE WATCHLIST -----------------------------------------




# # ------------------------ OPEN HIST-LIVE DATA -----------------------------------------

# INTERVAL_MAP = {
#     "ONE_MINUTE": 1,
#     "TWO_MINUTE": 2,
#     "THREE_MINUTE": 3,
#     "FOUR_MINUTE": 4,
#     "FIVE_MINUTE": 5,
#     "TEN_MINUTE": 10,
#     "FIFTEEN_MINUTE": 15,
#     "THIRTY_MINUTE": 30,
#     "SEVENTYFIVE_MINUTE": 75,
#     "ONETWENTYFIVE_MINUTE": 175,
#     "ONE_HOUR": 60,
#     "TWO_HOUR": 120,
#     "THREE_HOUR": 180,
#     "FOUR_HOUR": 240,
#     "ONE_DAY": 1440,
#     "ONE_WEEK": 10080,
#     "ONE_MONTH": 43200,
# }

# ohlc_buckets = defaultdict(lambda: defaultdict(lambda: {
#     "open": None,
#     "high": float("-inf"),
#     "low": float("inf"),
#     "close": None,
#     "volume": 0,
#     "start_time": None
# }))

# def calculate_ohlcv(token, timestamp, last_traded_price, volume, interval):
#     """Calculates OHLCV data dynamically based on the interval."""
#     interval_duration = INTERVAL_MAP[interval]
#     bucket_key = int(timestamp // interval_duration)  # Identify the interval bucket
#     ohlc = ohlc_buckets[token][bucket_key]  # Access the relevant bucket
    
#     # If the interval is starting, initialize `open` and `start_time`
#     if ohlc["start_time"] is None:
#         ohlc["open"] = last_traded_price
#         ohlc["start_time"] = bucket_key * interval_duration

#     # Update OHLC data
#     ohlc["high"] = max(ohlc["high"], last_traded_price)
#     ohlc["low"] = min(ohlc["low"], last_traded_price)
#     ohlc["close"] = last_traded_price
#     ohlc["volume"] += volume

#     # Check if the interval has ended
#     current_time = time.time()
#     if current_time >= ohlc["start_time"] + interval_duration:
#         # Emit the OHLC data
#         completed_ohlc = {
#             "type": "ohlcvdata",
#             "token": token,
#             "interval": interval,
#             "open": ohlc["open"],
#             "high": ohlc["high"],
#             "low": ohlc["low"],
#             "close": ohlc["close"],
#             "volume": ohlc["volume"],
#             "start_time": ohlc["start_time"],
#             "end_time": ohlc["start_time"] + interval_duration,
#         }

#         # Reset the bucket for the next interval
#         del ohlc_buckets[token][bucket_key]

#         return completed_ohlc
#     return None

# # Usage in the WebSocket Handler
# def parse_ohlcvdata_response(binary_data, interval):
#     """Parses the binary data and calculates OHLC data for the interval."""
#     try:
#         token = _parse_token_value(binary_data[2:27])
#         timestamp = unpack_data(binary_data, 123, 131, byte_format="q")
#         last_traded_price = unpack_data(binary_data, 43, 51, byte_format="q") / 100.0
#         volume = unpack_data(binary_data, 51, 59, byte_format="q")
        
#         # Calculate OHLCV
#         ohlcv_data = calculate_ohlcv(token, timestamp, last_traded_price, volume, interval)
#         return ohlcv_data
#     except Exception as e:
#         print(f"Error parsing OHLCV data response: {e}")
#         return None


# def fetch_historical_data(token, base_interval):
#     """Fetch historical data directly from Angel One API."""
#     todate = datetime.now().strftime("%Y-%m-%d %H:%M")
#     fromdate = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d %H:%M")

#     conn = http.client.HTTPSConnection("apiconnect.angelone.in")

#     payload = f'''{{
#         "exchange": "NSE",
#         "symboltoken": "{token}",
#         "interval": "{base_interval}",
#         "fromdate": "{fromdate}",
#         "todate": "{todate}"
#     }}'''

#     headers = {
#         'X-PrivateKey': '9H7XvPCk',
#         'Accept': 'application/json',
#         'X-SourceID': 'WEB',
#         'X-ClientLocalIP': 'CLIENT_LOCAL_IP',
#         'X-ClientPublicIP': 'CLIENT_PUBLIC_IP',
#         'X-MACAddress': 'MAC_ADDRESS',
#         'X-UserType': 'USER',
#         'Authorization': 'Bearer eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6IkE1Mzc0MTIiLCJyb2xlcyI6MCwidXNlcnR5cGUiOiJVU0VSIiwidG9rZW4iOiJleUpoYkdjaU9pSlNVekkxTmlJc0luUjVjQ0k2SWtwWFZDSjkuZXlKMWMyVnlYM1I1Y0dVaU9pSmpiR2xsYm5RaUxDSjBiMnRsYmw5MGVYQmxJam9pZEhKaFpHVmZZV05qWlhOelgzUnZhMlZ1SWl3aVoyMWZhV1FpT2pFeExDSnpiM1Z5WTJVaU9pSXpJaXdpWkdWMmFXTmxYMmxrSWpvaVlqWmhPVGRqWVdNdE4yUmlPUzB6WVdVeExUZzNaV1F0Wm1KbU9ERmtNV1JpTVRBNElpd2lhMmxrSWpvaWRISmhaR1ZmYTJWNVgzWXhJaXdpYjIxdVpXMWhibUZuWlhKcFpDSTZNVEVzSW5CeWIyUjFZM1J6SWpwN0ltUmxiV0YwSWpwN0luTjBZWFIxY3lJNkltRmpkR2wyWlNKOWZTd2lhWE56SWpvaWRISmhaR1ZmYkc5bmFXNWZjMlZ5ZG1salpTSXNJbk4xWWlJNklrRTFNemMwTVRJaUxDSmxlSEFpT2pFM016STBNalV6Tmprc0ltNWlaaUk2TVRjek1qTXpPRGM0T1N3aWFXRjBJam94TnpNeU16TTROemc1TENKcWRHa2lPaUkwTXpBeFpEbGtaQzFqT0RnekxUUmlZMkV0WWpJd015MWxObVZrTjJJeFl6TXpZelVpTENKVWIydGxiaUk2SWlKOS5ZRjV3YlVKQnBKcVRYbFA5ZGFYZElienQwRm9qSjVnOFk2MHZlR3d4eV9ZZllxakVPeDNBVXNOWU03VVZrVUtKXzVGRUMwdzB4eVQ5MERHVmNBOHlQRFpzTnlKdE4ycGxYOVNiUHRuOEdIOFY4d056VVNTSGFGZEtmbzFzSldNckNPQldoc3BGOUlmdjU3a1BVZGVBeTgyVGFWSWo4cGU5NUtfQWZTTGY2QlkiLCJBUEktS0VZIjoiOUg3WHZQQ2siLCJpYXQiOjE3MzIzMzg5NjksImV4cCI6MTczMjQyNTM2OX0._uhTQbOFIFjtw_GxRrake54dTIOrQS6aklPtIAhp88qK0SfNBzsoeW1Zkfbvrs5SHJdf3n5rzL6R4vBKe1FXWQ',
#         'Content-Type': 'application/json'
#     }

#     conn.request("POST", "/rest/secure/angelbroking/historical/v1/getCandleData", payload, headers)
#     res = conn.getresponse()
#     data = res.read()

#     response_data = data.decode("utf-8")
#     json_data = json.loads(response_data)

#     if json_data.get("status"):
#         return json_data.get("data", [])
#     else:
#         raise ValueError("Failed to fetch data from Angel One API")


# def aggregate_to_custom_interval(candles, target_interval_minutes, base_interval_minutes):
#     """Aggregate candles into the specified interval."""
#     aggregated_candles = []
#     group_duration = timedelta(minutes=target_interval_minutes)
#     current_group = []
#     current_start_time = None

#     for candle in candles:
#         candle_time = datetime.strptime(candle[0], "%Y-%m-%dT%H:%M:%S%z")
        
#         if not current_start_time:
#             current_start_time = candle_time

#         if candle_time >= current_start_time + group_duration:
#             # Finalize the current group
#             aggregated_candles.append({
#                 "date": current_start_time.isoformat(),
#                 "open": current_group[0]["open"],
#                 "high": max(c["high"] for c in current_group),
#                 "low": min(c["low"] for c in current_group),
#                 "close": current_group[-1]["close"],
#                 "volume": sum(c["volume"] for c in current_group)
#             })
#             # Start a new group
#             current_start_time = candle_time
#             current_group = []

#         current_group.append({
#             "date": candle[0],
#             "open": candle[1],
#             "high": candle[2],
#             "low": candle[3],
#             "close": candle[4],
#             "volume": candle[5]
#         })

#     # Finalize the last group if it has data
#     if current_group:
#         aggregated_candles.append({
#             "date": current_start_time.isoformat(),
#             "open": current_group[0]["open"],
#             "high": max(c["high"] for c in current_group),
#             "low": min(c["low"] for c in current_group),
#             "close": current_group[-1]["close"],
#             "volume": sum(c["volume"] for c in current_group)
#         })

#     return aggregated_candles


# def fetch_and_aggregate(token, target_interval):
#     """Fetch data at base interval and aggregate it to the target interval."""
#     base_interval = "ONE_MINUTE"  # Base interval for fetching
#     interval_minutes_map = {
#         "ONE_MINUTE": 1,
#         "TWO_MINUTE": 2,
#         "THREE_MINUTE": 3,
#         "FOUR_MINUTE": 4,
#         "FIVE_MINUTE": 5,
#         "TEN_MINUTE": 10,
#         "FIFTEEN_MINUTE": 15,
#         "THIRTY_MINUTE": 30,
#         "SEVENTYFIVE_MINUTE": 75,
#         "ONETWENTYFIVE_MINUTE": 175,
#         "ONE_HOUR": 60,
#         "TWO_HOUR": 120,
#         "THREE_HOUR": 180,
#         "FOUR_HOUR": 240,
#         "ONE_DAY": 1440,
#         "ONE_WEEK": 10080,
#         "ONE_MONTH": 43200,
#     }

#     target_minutes = interval_minutes_map[target_interval]
#     base_minutes = interval_minutes_map[base_interval]

#     if target_minutes < base_minutes or target_minutes % base_minutes != 0:
#         raise ValueError("Invalid base interval for aggregation")

#     raw_candles = fetch_historical_data(token, base_interval)
#     return aggregate_to_custom_interval(raw_candles, target_minutes, base_minutes)

# # ------------------------ CLOSE HIST-LIVE DATA -----------------------------------------





# # -------------------------- OPEN OPTION DATA -----------------------------------------


# def parse_optiondata_response(binary_data, expiry):
#     print("expiry:",expiry)
#     try:
#         token = _parse_token_value(binary_data[2:27])
#         strike_price = unpack_data(binary_data, 51, 59, byte_format="q") / 100.0
#         option_type = binary_data[59:60].decode("utf-8")  # Assuming 'C' or 'P' as a single byte
#         last_traded_price = unpack_data(binary_data, 43, 51, byte_format="q") / 100.0
#         open_interest = unpack_data(binary_data, 75, 83, byte_format="q")
#         return {
#             "type": "optiondata",
#             "token": token,
#             "strike_price": strike_price,
#             "option_type": option_type,
#             "last_traded_price": last_traded_price,
#             "open_interest": open_interest,
#         }
#     except Exception as e:
#         print(f"Error parsing option data response: {e}")
#         return None


# # -------------------------- CLOSE OPTION DATA -----------------------------------------


# # Build Subscription Request
# def build_angel_request(action, tokens):

#     df = fetch_data()
#     if df is not None:
#         exch_seg = df[df['token'].isin(tokens)][['exch_seg']]
#         if not exch_seg.empty:
#             exchange_type = EXCHANGE_TYPE_MAP.get(exch_seg.iloc[0]['exch_seg'], 1)
#             print("exch:", exchange_type)

#         return json.dumps({
#         "action": 1 if action == "subscribe" else 0,
#         "params": {
#             "mode": 3,
#             "tokenList": [{"exchangeType": exchange_type, "tokens": tokens}],
#         },
#     })


# # WebSocket Handlers
# async def handle_angel_ws(subscribe_for, tokens, interval, expiry, response_queue, ping_interval=20, ping_timeout=10):
#     """Handles Angel One WebSocket connections with heartbeat mechanism."""
#     if subscribe_for == "ohlcvdata":
#         for token in tokens:
#             historical_data = fetch_and_aggregate(token, interval)
#             await response_queue.put(historical_data)  # Send historical data to the client

#     try:
#         async with websockets.connect(URL, extra_headers=HEADERS, ping_interval=None, ping_timeout=ping_timeout) as angel_ws:  # Disable built-in ping
#             print(f"WebSocket connection established for {subscribe_for}.")

#             # Subscribe to tokens
#             request = build_angel_request("subscribe", tokens)
#             await angel_ws.send(request)
#             print(f"Subscribed to {subscribe_for} with tokens: {tokens}")

#             async def send_heartbeat():
#                 while True:
#                     try:
#                         await asyncio.sleep(ping_interval)
#                         await angel_ws.send("ping")
#                         print("Sent heartbeat: ping")
#                     except Exception as e:
#                         print(f"Failed to send heartbeat: {e}")
#                         break  # Exit if sending fails

#             async def receive_messages():
#                 while True:
#                     try:
#                         binary_message = await angel_ws.recv()
#                         if isinstance(binary_message, str) and binary_message == "pong":
#                             print("Received heartbeat response: pong")
#                             continue

#                         # Handle other incoming data
#                         if isinstance(binary_message, bytes):
#                             if subscribe_for == "watchlist":
#                                 parsed_data = parse_watchlist_response(binary_message)
#                             elif subscribe_for == "ohlcvdata":
#                                 parsed_data = parse_ohlcvdata_response(binary_message, interval)
#                             elif subscribe_for == "optiondata":
#                                 parsed_data = parse_optiondata_response(binary_message, expiry)

#                             if parsed_data:
#                                 await response_queue.put(parsed_data)
#                     except Exception as e:
#                         print(f"Error receiving message: {e}")
#                         break  # Exit if receiving fails

#             # Run heartbeat and message receiving concurrently
#             await asyncio.gather(send_heartbeat(), receive_messages())

#     except Exception as e:
#         print(f"Error in Angel WebSocket for {subscribe_for}: {e}")
#         raise  # Propagate the exception for reconnect to handle

# @app.websocket("/ws")
# async def websocket_endpoint(websocket: WebSocket):
#     await websocket.accept()
#     response_queue = asyncio.Queue()

#     async def receive_from_client():
#         async for message in websocket.iter_text():
#             try:
#                 client_request = json.loads(message)
#                 subscribe_for = client_request.get("subscribe_for")
#                 action = client_request.get("action")
#                 tokens = client_request.get("tokens", [])
#                 interval = client_request.get("interval", None)
#                 expiry = client_request.get("expiry", None)
#                 if subscribe_for and action == "subscribe" and tokens:
#                     asyncio.create_task(handle_angel_ws(subscribe_for, tokens, interval, expiry, response_queue))
#             except json.JSONDecodeError:
#                 print("Invalid JSON received from client.")

#     async def send_to_client():
#         while True:
#             response = await response_queue.get()
#             await websocket.send_text(json.dumps(response))

#     await asyncio.gather(receive_from_client(), send_to_client())
# ----------------------------------------------------------------------------------------------




import struct
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import websockets
import aiohttp
import requests
import pandas as pd
from cachetools import cached, TTLCache
import http.client
import datetime
from datetime import datetime, timedelta
import time
from collections import defaultdict



app = FastAPI()


# Add CORS Middleware for WebSocket compatibility
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for testing; restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

URL = "wss://smartapisocket.angelone.in/smart-stream?clientCode=A537412&feedToken=eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6IkE1Mzc0MTIiLCJpYXQiOjE3MzI2ODIzNjYsImV4cCI6MTczMjc2ODc2Nn0.CJpzXl7QLpUaDxdtQ_1CVa13Q4QJhR86nnJ8gx6FRAQ3WT_Lhc4PcARKhr3bBfIndQJTzc91F8UL4LlF5TV10g&apiKey=9H7XvPCk"
        
HEADERS = {
    'Authorization': 'Bearer eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6IkE1Mzc0MTIiLCJyb2xlcyI6MCwidXNlcnR5cGUiOiJVU0VSIiwidG9rZW4iOiJleUpoYkdjaU9pSlNVekkxTmlJc0luUjVjQ0k2SWtwWFZDSjkuZXlKMWMyVnlYM1I1Y0dVaU9pSmpiR2xsYm5RaUxDSjBiMnRsYmw5MGVYQmxJam9pZEhKaFpHVmZZV05qWlhOelgzUnZhMlZ1SWl3aVoyMWZhV1FpT2pFeExDSnpiM1Z5WTJVaU9pSXpJaXdpWkdWMmFXTmxYMmxrSWpvaVlqWmhPVGRqWVdNdE4yUmlPUzB6WVdVeExUZzNaV1F0Wm1KbU9ERmtNV1JpTVRBNElpd2lhMmxrSWpvaWRISmhaR1ZmYTJWNVgzWXhJaXdpYjIxdVpXMWhibUZuWlhKcFpDSTZNVEVzSW5CeWIyUjFZM1J6SWpwN0ltUmxiV0YwSWpwN0luTjBZWFIxY3lJNkltRmpkR2wyWlNKOWZTd2lhWE56SWpvaWRISmhaR1ZmYkc5bmFXNWZjMlZ5ZG1salpTSXNJbk4xWWlJNklrRTFNemMwTVRJaUxDSmxlSEFpT2pFM016STNOamczTmpZc0ltNWlaaUk2TVRjek1qWTRNakU0Tml3aWFXRjBJam94TnpNeU5qZ3lNVGcyTENKcWRHa2lPaUptTmpSbFl6SmpaUzFsTm1Nd0xUUmtOVE10WW1ZMU1pMHhaVEpsWW1VM016azBZMkVpTENKVWIydGxiaUk2SWlKOS5hLUoxaGo4NVo2d21VdVRGS1YxOG5Lc2F3Z1l3R0dOTHNieFRmTFRzMHAwVDI0MFNnSVNNT1lKb3pVY1Btb2phR2dpUVV3YlZTcmlXTk9pZ2NTd1FPUV9ld09wXzRvQkxEWXBxTmJBRXQyeUdiOHFUczRHUWE4Mkp6Q2xtOHBQQ21UVWoteGNkenVRNUN6QWdjVmVzZnNwbjc2ZG5JSS1fUlFuS0lBZUlTVkUiLCJBUEktS0VZIjoiOUg3WHZQQ2siLCJpYXQiOjE3MzI2ODIzNjYsImV4cCI6MTczMjc2ODc2Nn0.TImrKuBQBI4UcwTv-zqZO3uz9GDgMxBmq6ozl6jseW_PeRtHJtcACV5nX1MUqrBlU4BcGonekRCHkIRFiy1XeQ',
    "x-api-key": "9H7XvPCk",
    "x-feed-token": "eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6IkE1Mzc0MTIiLCJpYXQiOjE3MzI2ODIzNjYsImV4cCI6MTczMjc2ODc2Nn0.CJpzXl7QLpUaDxdtQ_1CVa13Q4QJhR86nnJ8gx6FRAQ3WT_Lhc4PcARKhr3bBfIndQJTzc91F8UL4LlF5TV10g"
}

EXCHANGE_TYPE_MAP = {
    "NSE": 1,
    "NFO": 2,
    "BSE": 3,
    "BFO": 4,
    "MCX": 5,

    "CDS": 7,
    "NCO": 13,
    "NCDEX": 11
}

EXCHANGE_RELATION_MAP = {
    1: 2,  # NSE -> NFO
    3: 4,  # BSE -> BFO
    5: 5   # MCX -> MCX (no change)
}

previous_oi = {}

# Utility Functions
def unpack_data(binary_data, start, end, byte_format="q"):
    """Unpacks a slice of binary data using the specified format."""
    relevant_bytes = binary_data[start:end]
    return struct.unpack(byte_format, relevant_bytes)[0]

def _parse_token_value(binary_packet):
    """Extracts the token from binary data."""
    token = ""
    for byte in binary_packet:
        char = chr(byte)
        if char == "\x00":
            return token
        token += char
    return token


# Cache for storing fetched data with a Time-To-Live (TTL) of 300 seconds (5 minutes)
cache = TTLCache(maxsize=1, ttl=300)
@cached(cache)
def fetch_data():
    api_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    try:
        headers = {"Accept-Encoding": "gzip"}  # Request compressed data
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        data = response.json()
        print("Fetched new data from API")  # For debugging to see when the API is called
        return pd.DataFrame(data)
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None



# -------------------------- OPEN WATCHLIST -----------------------------------------

# Parsing Functions
def parse_watchlist_response(binary_data):
    try:
        token = _parse_token_value(binary_data[2:27])
        last_traded_price = unpack_data(binary_data, 43, 51, byte_format="q") / 100.0
        closed_price = unpack_data(binary_data, 115, 123, byte_format="q") / 100.0

        if closed_price != 0:
            percent_change = round(((last_traded_price - closed_price) / closed_price) * 100, 2)
        else:
            percent_change = None

        df = fetch_data()
        if df is not None:
            symbol_df = df[df['token'].isin([token])][['symbol']]
            if not symbol_df.empty:
                symbol = symbol_df.iloc[0]['symbol']  
                print("exch:", symbol)


        return {
            "type": "watchlist",
            "token": token,
            "symbol": symbol,
            "ltp": last_traded_price,
            "percent_change": percent_change,
        }
    except Exception as e:
        print(f"Error parsing watchlist response: {e}")
        return None
    
# -------------------------- CLOSE WATCHLIST -----------------------------------------




# ------------------------ OPEN HIST-LIVE DATA -----------------------------------------

INTERVAL_MAP = {
    "ONE_MINUTE": 1,
    "TWO_MINUTE": 2,
    "THREE_MINUTE": 3,
    "FOUR_MINUTE": 4,
    "FIVE_MINUTE": 5,
    "TEN_MINUTE": 10,
    "FIFTEEN_MINUTE": 15,
    "THIRTY_MINUTE": 30,
    "SEVENTYFIVE_MINUTE": 75,
    "ONETWENTYFIVE_MINUTE": 175,
    "ONE_HOUR": 60,
    "TWO_HOUR": 120,
    "THREE_HOUR": 180,
    "FOUR_HOUR": 240,
    "ONE_DAY": 1440,
    "ONE_WEEK": 10080,
    "ONE_MONTH": 43200,
}


ohlc_buckets = defaultdict(lambda: defaultdict(lambda: {
    "open": None,
    "high": float('-inf'),
    "low": float('inf'),
    "close": None,
    "volume": 0,
    "start_time": None,
}))

INTERVAL_MIN_MAP = {
    "ONE_MINUTE": 60,
    "TWO_MINUTE": 120,
    "FIVE_MINUTES": 300,
    "FIFTEEN_MINUTES": 900,
}



# --------------   calculated live OHLCV data -----------------------
# def calculate_ohlcv(token, timestamp, last_traded_price, volume, interval):
#     """
#     Calculate OHLCV dynamically based on the interval, reset OHLC data for new intervals.
#     """
#     interval_duration = INTERVAL_MIN_MAP.get(interval, 60)  # Default to 1-minute
#     bucket_key = int(timestamp // interval_duration)  # Identify the interval bucket
    
#     # Access or initialize the relevant bucket
#     ohlc = ohlc_buckets[token][bucket_key]
    
#     # Initialize the bucket if it's the first data point in this interval
#     if ohlc["start_time"] is None:
#         ohlc["open"] = last_traded_price
#         ohlc["start_time"] = bucket_key * interval_duration
    
#     # Update OHLC data
#     ohlc["high"] = max(ohlc["high"], last_traded_price)
#     ohlc["low"] = min(ohlc["low"], last_traded_price)
#     ohlc["close"] = last_traded_price
#     ohlc["volume"] += volume

#     # Check if the interval has ended
#     current_time = time.time()
#     if current_time >= ohlc["start_time"] + interval_duration:
#         # Format the `start_time` to a date string
#         date_str = datetime.fromtimestamp(ohlc["start_time"]).strftime("%Y-%m-%dT%H:%M:%S")
        
#         # Emit the completed OHLC data
#         completed_ohlc = {
#             "date": date_str,
#             "open": ohlc["open"],
#             "high": ohlc["high"],
#             "low": ohlc["low"],
#             "close": ohlc["close"],
#             "volume": ohlc["volume"],
#         }
        
#         # Remove the completed bucket
#         del ohlc_buckets[token][bucket_key]
        
#         # Reset the OHLC bucket for the new interval
#         ohlc_buckets[token][bucket_key] = {
#             "open": last_traded_price,
#             "high": last_traded_price,
#             "low": last_traded_price,
#             "close": last_traded_price,
#             "volume": volume,
#             "start_time": bucket_key * interval_duration + interval_duration,
#         }
        
#         return completed_ohlc
    
#     return None


# ------------------------ live tick data ohlcv ------------------------
tick_data = {}

def calculate_ohlcv(token, timestamp, last_traded_price, volume):
    """
    Track live tick data (date, open, low, high, close, volume).
    """
    # Convert the timestamp to date
    date_str = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%dT%H:%M:%S")
    
    # Initialize tick data for the token if it doesn't exist
    if token not in tick_data:
        tick_data[token] = {
            "date": date_str,
            "open": last_traded_price,
            "low": last_traded_price,
            "high": last_traded_price,
            "close": last_traded_price,
            "volume": volume
        }
    else:
        # Update tick data for the token
        tick_data[token]["date"] = date_str
        tick_data[token]["high"] = max(tick_data[token]["high"], last_traded_price)
        tick_data[token]["low"] = min(tick_data[token]["low"], last_traded_price)
        tick_data[token]["close"] = last_traded_price
        tick_data[token]["volume"] += volume
    
    # Return the updated live tick data
    return tick_data[token]



# Usage in the WebSocket Handler
def parse_ohlcvdata_response(binary_data, interval):
    """Parses the binary data and calculates OHLC data for the interval."""
    try:
        token = _parse_token_value(binary_data[2:27])
        timestamp = unpack_data(binary_data, 123, 131, byte_format="q")
        last_traded_price = unpack_data(binary_data, 43, 51, byte_format="q") / 100.0
        volume = unpack_data(binary_data, 51, 59, byte_format="q")
        
        # Calculate OHLCV
        # ohlcv_data = calculate_ohlcv(token, timestamp, last_traded_price, volume, interval)
        ohlcv_data = calculate_ohlcv(token, timestamp, last_traded_price, volume)
        return ohlcv_data
    except Exception as e:
        print(f"Error parsing OHLCV data response: {e}")
        return None


def fetch_historical_data(token, base_interval):
    """Fetch historical data directly from Angel One API."""
    todate = datetime.now().strftime("%Y-%m-%d %H:%M")
    fromdate = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M")

    conn = http.client.HTTPSConnection("apiconnect.angelone.in")

    print("token type:",type(token))
    df = fetch_data()
    if df is not None:
        exch_seg = df[df['token'].isin([token])][['exch_seg']]
        if not exch_seg.empty:
            exch_seg_value = exch_seg.iloc[0]['exch_seg'] 

    payload = f'''{{
        "exchange": "{exch_seg_value}",
        "symboltoken": "{token}",
        "interval": "{base_interval}",
        "fromdate": "{fromdate}",
        "todate": "{todate}"
    }}'''

    headers = {
        'X-PrivateKey': '9H7XvPCk',
        'Accept': 'application/json',
        'X-SourceID': 'WEB',
        'X-ClientLocalIP': 'CLIENT_LOCAL_IP',
        'X-ClientPublicIP': 'CLIENT_PUBLIC_IP',
        'X-MACAddress': 'MAC_ADDRESS',
        'X-UserType': 'USER',
        'Authorization': 'Bearer eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6IkE1Mzc0MTIiLCJyb2xlcyI6MCwidXNlcnR5cGUiOiJVU0VSIiwidG9rZW4iOiJleUpoYkdjaU9pSlNVekkxTmlJc0luUjVjQ0k2SWtwWFZDSjkuZXlKMWMyVnlYM1I1Y0dVaU9pSmpiR2xsYm5RaUxDSjBiMnRsYmw5MGVYQmxJam9pZEhKaFpHVmZZV05qWlhOelgzUnZhMlZ1SWl3aVoyMWZhV1FpT2pFeExDSnpiM1Z5WTJVaU9pSXpJaXdpWkdWMmFXTmxYMmxrSWpvaVlqWmhPVGRqWVdNdE4yUmlPUzB6WVdVeExUZzNaV1F0Wm1KbU9ERmtNV1JpTVRBNElpd2lhMmxrSWpvaWRISmhaR1ZmYTJWNVgzWXhJaXdpYjIxdVpXMWhibUZuWlhKcFpDSTZNVEVzSW5CeWIyUjFZM1J6SWpwN0ltUmxiV0YwSWpwN0luTjBZWFIxY3lJNkltRmpkR2wyWlNKOWZTd2lhWE56SWpvaWRISmhaR1ZmYkc5bmFXNWZjMlZ5ZG1salpTSXNJbk4xWWlJNklrRTFNemMwTVRJaUxDSmxlSEFpT2pFM016STJPREk1T0Rnc0ltNWlaaUk2TVRjek1qVTVOalF3T0N3aWFXRjBJam94TnpNeU5UazJOREE0TENKcWRHa2lPaUl4TVRZeE5UWTROaTAyTURjNUxUUXdNREV0WVdWa1l5MWpaR1ZrTlRZek9UUTVOemNpTENKVWIydGxiaUk2SWlKOS5kbzJ5VjZUT2hvTC1YanpIOXFHOHVCSVU0QVZWNjdpb1EwcXZJc2phdUdXcGJaNjhMd3g4clNfZi0tSVBZVWt1SURjZEpOTlByX0FwenVsaXlYd3lZdTh5SnduQ1hZM3dlaXNNT2ZrLVlvN0k5bXJGWmY4X25TUzVvdUJxZkNBNDdsUTlXRE9EbkVFVFRwdlVDd2F0N0YyYTdwNmJoaWk2TVR5bEZWS3pnd2ciLCJBUEktS0VZIjoiOUg3WHZQQ2siLCJpYXQiOjE3MzI1OTY1ODgsImV4cCI6MTczMjY4Mjk4OH0.AhIRSlL6hMcKrXXgSlh-5-XdODCuzBxHBwd47U6LWmPtLMa8uE7WatgWpXK7LD5PjbiQYtaDuneLHVtQg4tLUA',
        'Content-Type': 'application/json'
    }

    conn.request("POST", "/rest/secure/angelbroking/historical/v1/getCandleData", payload, headers)
    res = conn.getresponse()
    data = res.read()

    response_data = data.decode("utf-8")
    json_data = json.loads(response_data)

    if json_data.get("status"):
        return json_data.get("data", [])
    else:
        raise ValueError("Failed to fetch data from Angel One API")


def aggregate_to_custom_interval(candles, target_interval_minutes, base_interval_minutes):
    """Aggregate candles into the specified interval."""
    aggregated_candles = []
    group_duration = timedelta(minutes=target_interval_minutes)
    current_group = []
    current_start_time = None

    for candle in candles:
        candle_time = datetime.strptime(candle[0], "%Y-%m-%dT%H:%M:%S%z")
        
        if not current_start_time:
            current_start_time = candle_time

        if candle_time >= current_start_time + group_duration:
            # Finalize the current group
            aggregated_candles.append({
                "date": current_start_time.isoformat(),
                "open": current_group[0]["open"],
                "high": max(c["high"] for c in current_group),
                "low": min(c["low"] for c in current_group),
                "close": current_group[-1]["close"],
                "volume": sum(c["volume"] for c in current_group)
            })
            # Start a new group
            current_start_time = candle_time
            current_group = []

        current_group.append({
            "date": candle[0],
            "open": candle[1],
            "high": candle[2],
            "low": candle[3],
            "close": candle[4],
            "volume": candle[5]
        })

    # Finalize the last group if it has data
    if current_group:
        aggregated_candles.append({
            "date": current_start_time.isoformat(),
            "open": current_group[0]["open"],
            "high": max(c["high"] for c in current_group),
            "low": min(c["low"] for c in current_group),
            "close": current_group[-1]["close"],
            "volume": sum(c["volume"] for c in current_group)
        })

    return {
        "historical_data": aggregated_candles
    }


def fetch_and_aggregate(token, target_interval):
    """Fetch data at base interval and aggregate it to the target interval."""
    base_interval = "ONE_MINUTE"  # Base interval for fetching
    interval_minutes_map = {
        "ONE_MINUTE": 1,
        "TWO_MINUTE": 2,
        "THREE_MINUTE": 3,
        "FOUR_MINUTE": 4,
        "FIVE_MINUTE": 5,
        "TEN_MINUTE": 10,
        "FIFTEEN_MINUTE": 15,
        "THIRTY_MINUTE": 30,
        "SEVENTYFIVE_MINUTE": 75,
        "ONETWENTYFIVE_MINUTE": 175,
        "ONE_HOUR": 60,
        "TWO_HOUR": 120,
        "THREE_HOUR": 180,
        "FOUR_HOUR": 240,
        "ONE_DAY": 1440,
        "ONE_WEEK": 10080,
        "ONE_MONTH": 43200,
    }

    target_minutes = interval_minutes_map[target_interval]
    base_minutes = interval_minutes_map[base_interval]

    if target_minutes < base_minutes or target_minutes % base_minutes != 0:
        raise ValueError("Invalid base interval for aggregation")

    raw_candles = fetch_historical_data(token, base_interval)
    return aggregate_to_custom_interval(raw_candles, target_minutes, base_minutes)

# ------------------------ CLOSE HIST-LIVE DATA -----------------------------------------





# -------------------------- OPEN OPTION DATA -----------------------------------------
def get_symbol(token):
    df = fetch_data()
    if df is not None:
        if isinstance(token, str):  
            token = [token]  # Convert single string to list
        elif not isinstance(token, (list, pd.Series)):
            raise ValueError("Invalid type for token. Must be a string, list, or pandas Series.")
        symbols = df[df['token'].isin(token)]['symbol'].tolist()
        return symbols[0] if symbols else ""
    return ""

def get_strick(token):
    df = fetch_data()
    if df is not None:
        if isinstance(token, str):  
            token = [token]  # Convert single string to list
        elif not isinstance(token, (list, pd.Series)):
            raise ValueError("Invalid type for token. Must be a string, list, or pandas Series.")
        df['strike'] = pd.to_numeric(df['strike'], errors='coerce')
        strike = df[df['token'].isin(token)]['strike'].tolist()
        if strike:
            formatted_strike = strike[0] / 100
            return str(int(formatted_strike)) if formatted_strike.is_integer() else str(formatted_strike)
    return ""

def get_lotsize(token):
    df = fetch_data()
    if df is not None:
        if isinstance(token, str):  
            token = [token]  # Convert single string to list
        elif not isinstance(token, (list, pd.Series)):
            raise ValueError("Invalid type for token. Must be a string, list, or pandas Series.")
        lotsize = df[df['token'].isin(token)]['lotsize'].tolist()
        return lotsize[0] if lotsize else ""
    return ""

# def parse_optiondata_response(binary_data):

#     try:
#         token = _parse_token_value(binary_data[2:27])
#         last_traded_price = unpack_data(binary_data, 43, 51, byte_format="q") / 100.0
#         open_interest = unpack_data(binary_data, 131, 139, byte_format="q")
#         closed_price = unpack_data(binary_data, 115, 123, byte_format="q")/100.0

#         if closed_price != 0:
#             percent_change = round(((last_traded_price - closed_price) / closed_price) * 100, 2)
#         else:
#             percent_change = 0.0


#         global previous_oi
#         open_interest_previous = previous_oi.get(token, None)
#         if open_interest_previous is not None:
#             oi_change = open_interest - open_interest_previous
#             if open_interest_previous != 0:
#                 oi_percent_change = round((oi_change / open_interest_previous) * 100, 2)
#             else:
#                 oi_percent_change = 0.0
#         else:
#             oi_change = 0 
#             oi_percent_change = 0.0
#         previous_oi[token] = open_interest

#         symbol = get_symbol(token)
#         strike = get_strick(token)
#         lot_size = get_lotsize(token)

#         if "PE" in symbol:
#             response_data = {
#                 "pe_symbol": symbol,
#                 "pe_token": token,
#                 "pe_strike": strike,
#                 "pe_ltp": last_traded_price,
#                 "pe_percent_change": percent_change,
#                 "pe_open_interest": open_interest,
#                 "pe_oi_change": oi_change,
#                 "pe_oi_percent_change": oi_percent_change,
#                 "lot_size": lot_size
#             }
#         elif "CE" in symbol:
#             response_data = {
#                 "ce_symbol": symbol,
#                 "ce_token": token,
#                 "ce_strike": strike,
#                 "ce_ltp": last_traded_price,
#                 "ce_percent_change": percent_change,
#                 "ce_open_interest": open_interest,
#                 "ce_oi_change": oi_change,
#                 "ce_oi_percent_change": oi_percent_change,
#                 "lot_size": lot_size
#             }
#         else:
#             # Default response for unsupported symbols
#             response_data = {
#                 "symbol": symbol,
#                 "token": token,
#                 "ltp": last_traded_price
#             }
#         return response_data
#     except Exception as e:
#         print(f"Error parsing option data response: {e}")
#         return None
def parse_optiondata_response(binary_data):
    try:
        token = _parse_token_value(binary_data[2:27])
        last_traded_price = unpack_data(binary_data, 43, 51, byte_format="q") / 100.0
        open_interest = unpack_data(binary_data, 131, 139, byte_format="q")
        closed_price = unpack_data(binary_data, 115, 123, byte_format="q") / 100.0

        if closed_price != 0:
            percent_change = round(((last_traded_price - closed_price) / closed_price) * 100, 2)
        else:
            percent_change = 0.0

        global previous_oi
        open_interest_previous = previous_oi.get(token, None)
        
        # Only calculate open interest change if previous open interest exists
        if open_interest_previous is not None:
            oi_change = open_interest - open_interest_previous
            if open_interest_previous != 0:
                oi_percent_change = round((oi_change / open_interest_previous) * 100, 2)
            else:
                oi_percent_change = 0.0
        else:
            oi_change = 0
            oi_percent_change = 0.0
        
        # Update previous open interest with current value
        previous_oi[token] = open_interest

        symbol = get_symbol(token)
        strike = get_strick(token)
        lot_size = get_lotsize(token)

        if "PE" in symbol:
            response_data = {
                "pe_symbol": symbol,
                "pe_token": token,
                "pe_strike": strike,
                "pe_ltp": last_traded_price,
                "pe_percent_change": percent_change,
                "pe_open_interest": open_interest,
                "pe_oi_change": oi_change,
                "pe_oi_percent_change": oi_percent_change,
                "lot_size": lot_size
            }
        elif "CE" in symbol:
            response_data = {
                "ce_symbol": symbol,
                "ce_token": token,
                "ce_strike": strike,
                "ce_ltp": last_traded_price,
                "ce_percent_change": percent_change,
                "ce_open_interest": open_interest,
                "ce_oi_change": oi_change,
                "ce_oi_percent_change": oi_percent_change,
                "lot_size": lot_size
            }
        else:
            # Default response for unsupported symbols
            response_data = {
                "symbol": symbol,
                "token": token,
                "ltp": last_traded_price
            }

        return response_data

    except Exception as e:
        print(f"Error parsing option data response: {e}")
        return None



# -------------------------- CLOSE OPTION DATA -----------------------------------------


# -------------------------- OPEN LTPDATA -----------------------------------------

# Parsing Functions
def parse_ltpdata_response(binary_data):
    try:
        token = _parse_token_value(binary_data[2:27])
        last_traded_price = unpack_data(binary_data, 43, 51, byte_format="q") / 100.0
        closed_price = unpack_data(binary_data, 115, 123, byte_format="q") / 100.0

        if closed_price != 0:
            percent_change = round(((last_traded_price - closed_price) / closed_price) * 100, 2)
        else:
            percent_change = None

        df = fetch_data()
        if df is not None:
            filtered_df = df[df['token'].isin([token])]
            if not filtered_df.empty:
                symbol = filtered_df.iloc[0]['symbol']
                name = filtered_df.iloc[0]['name'] if 'name' in filtered_df.columns else None
                lotsize = filtered_df.iloc[0]['lotsize'] if 'lotsize' in filtered_df.columns else None                
                print(f"Symbol: {symbol}, Name: {name}, Lot Size: {lotsize}")



        return {
            "type": "ltpdata",
            "token": token,
            "symbol": symbol,
            "lotsize":lotsize,
            "name": name,
            "ltp": last_traded_price,
            "percent_change": percent_change,
        }
    except Exception as e:
        print(f"Error parsing ltpdata response: {e}")
        return None
    
# -------------------------- CLOSE LTPDATA -----------------------------------------

def build_angel_request(action, tokens, tokens_list=None):
    df = fetch_data()
    if df is not None:
        exch_seg = df[df['token'].isin(tokens)][['exch_seg']]
        if not exch_seg.empty:
            exchange_type = EXCHANGE_TYPE_MAP.get(exch_seg.iloc[0]['exch_seg'], 1)

            if tokens_list:
                related_exchange_type = EXCHANGE_RELATION_MAP.get(exchange_type, exchange_type)
            else:
                related_exchange_type = None

            token_list = [{"exchangeType": exchange_type, "tokens": tokens}]
            if tokens_list and related_exchange_type:
                token_list.append({"exchangeType": related_exchange_type, "tokens": tokens_list})

            return json.dumps({
                "action": 1 if action == "subscribe" else 0,
                "params": {
                    "mode": 3,
                    "tokenList": token_list,
                },
            })



# WebSocket Handlers
async def handle_angel_ws(subscribe_for, tokens, interval, expiry, response_queue, ping_interval=20, ping_timeout=10):
    
    if subscribe_for == "ohlcvdata":
        for token in tokens:
            historical_data = fetch_and_aggregate(token, interval)
            await response_queue.put(historical_data)  # Send historical data to the client


    if subscribe_for == "optiondata":
        df = fetch_data()  # Assuming fetch_data() returns a DataFrame
        if df is not None:
            # Get the name corresponding to the tokens
            name_row = df[df['token'].isin(tokens)][['name']]
            if not name_row.empty:
                name = name_row.iloc[0]['name']

                # Filter data based on the given conditions
                filtered_data = df[
                    (df["symbol"].str.contains(r"CE|PE", na=False)) &
                    (df["exch_seg"].isin(["NFO", "BFO", "MCX"])) &
                    (df["expiry"] == expiry) &
                    (df["name"] == name) &
                    (df["instrumenttype"].isin(["OPTSTK", "OPTIDX", "OPTFUT"]))
                ]

                # Select the desired columns: token, strike, and symbol
                option_data = filtered_data[["token", "strike", "symbol"]]
                tokens_list = option_data["token"].tolist()
                print("lst:",tokens_list)

    try:
        async with websockets.connect(URL, ping_interval=None, ping_timeout=ping_timeout) as angel_ws:  # Disable built-in ping
            print(f"WebSocket connection established for {subscribe_for}.")

            # Subscribe to tokens
            if subscribe_for == "optiondata":
                request = build_angel_request("subscribe", tokens, tokens_list)  # Include tokens_list
            else:
                request = build_angel_request("subscribe", tokens)  # Exclude tokens_list for watchlist,ltpdata

            await angel_ws.send(request)


            async def send_heartbeat():
                while True:
                    try:
                        await asyncio.sleep(ping_interval)
                        await angel_ws.send("ping")
                        print("Sent heartbeat: ping")
                    except Exception as e:
                        print(f"Failed to send heartbeat: {e}")
                        break  # Exit if sending fails

            async def receive_messages():
                while True:
                    try:
                        binary_message = await angel_ws.recv()
                        if isinstance(binary_message, str) and binary_message == "pong":
                            print("Received heartbeat response: pong")
                            continue

                        # Handle other incoming data
                        if isinstance(binary_message, bytes):
                            if subscribe_for == "watchlist":
                                parsed_data = parse_watchlist_response(binary_message)
                            elif subscribe_for == "ohlcvdata":
                                parsed_data = parse_ohlcvdata_response(binary_message, interval)
                            elif subscribe_for == "optiondata":
                                parsed_data = parse_optiondata_response(binary_message)
                            elif subscribe_for == "ltpdata":
                                parsed_data = parse_ltpdata_response(binary_message)

                            if parsed_data:
                                await response_queue.put(parsed_data)
                    except Exception as e:
                        print(f"Error receiving message: {e}")
                        break  # Exit if receiving fails

            # Run heartbeat and message receiving concurrently
            await asyncio.gather(send_heartbeat(), receive_messages())

    except Exception as e:
        print(f"Error in Angel WebSocket for {subscribe_for}: {e}")
        raise  # Propagate the exception for reconnect to handle

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    response_queue = asyncio.Queue()

    async def receive_from_client():
        async for message in websocket.iter_text():
            try:
                client_request = json.loads(message)
                subscribe_for = client_request.get("subscribe_for")
                action = client_request.get("action")
                tokens = client_request.get("tokens", [])
                interval = client_request.get("interval", None)
                expiry = client_request.get("expiry", None)
                if subscribe_for and action == "subscribe" and tokens:
                    asyncio.create_task(handle_angel_ws(subscribe_for, tokens, interval, expiry, response_queue))
            except json.JSONDecodeError:
                print("Invalid JSON received from client.")

    async def send_to_client():
        while True:
            response = await response_queue.get()
            await websocket.send_text(json.dumps(response))

    await asyncio.gather(receive_from_client(), send_to_client())











#-------------------------- HTTP METHODS --------------------------------


from fastapi import FastAPI, Query
import pandas as pd
import requests


# Function to load data dynamically
def load_scrip_master_data():
    url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
    response = requests.get(url)
    data = response.json()
    return pd.DataFrame(data)

# Load the data into memory when the API starts
data = load_scrip_master_data()

# Function to filter data by type
def filter_by_type(df, filter_type):
    """
    Filters the DataFrame based on the filter_type criteria.
    """
    if filter_type == "stocks":
        # Stocks: NSE/BSE with specific symbols
        stock_types = ["EQ", "BE", "BL", "IL", "SME", "T", "Z"]
        return df[(df["exch_seg"].isin(["NSE", "BSE"])) & (df["symbol"].str[-2:].isin(stock_types))]
    
    elif filter_type == "options":
        # option_types = ["OPTSTK", "OPTIDX", "OPTCUR", "OPTCOM", "OPTCIR"]
        option_types = ["OPTSTK"]
        return df[df["instrumenttype"].isin(option_types)]
    
    elif filter_type == "futures":
        # future_types = ["FUTSTK", "FUTIDX", "FUTCUR", "FUTCOM", "FUTIR"]
        future_types = ["FUTSTK"]
        return df[df["instrumenttype"].isin(future_types)]
    
    elif filter_type == "indexes":
        # index_types = ["INDEX", "SECTOR", "ETFIDX"]
        index_types = ["AMXIDX"]
        return df[df["instrumenttype"].isin(index_types)]
    
    elif filter_type == "commodities":
        commodity_types = ["FUTCOM"]
        return df[df["instrumenttype"].isin(commodity_types)]
    
    return df

@app.get("/api/search")
def search_symbol(
    q: str = Query(..., description="Search query for symbol or name"),
    offset: int = 0,
    limit: int = 5,
    filter_type: str = Query(None, description="Filter type: stocks, futures, options, indexes, commodities"),
):
    """
    Search for symbols and names in the dataset with optional filters.
    """
    # Reload data dynamically
    global data
    if data.empty:
        data = load_scrip_master_data()

    # Filter the data based on the search query
    filtered_data = data[data["symbol"].str.contains(q, case=False) | data["name"].str.contains(q, case=False)]
    
    # Apply additional filter if provided
    if filter_type:
        filtered_data = filter_by_type(filtered_data, filter_type)
    
    # Apply offset and limit for pagination
    paginated_data = filtered_data.iloc[offset : offset + limit]

    # Convert the result to JSON format
    result = paginated_data.to_dict(orient="records")
    return {"count": len(filtered_data), "data": result}




from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import requests
import pandas as pd
from datetime import datetime
from typing import List, Optional


# Function to load the ScripMaster data from the URL
def load_scrip_master_data():
    url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
    response = requests.get(url)
    data = response.json()
    return pd.DataFrame(data)

# Load the data once when the application starts
scrip_master_data = load_scrip_master_data()

# Response model for the expiry dates
class ExpiryDateResponse(BaseModel):
    status: str
    message: Optional[str] = None
    data: Optional[List[str]] = None

@app.get("/api/expiry", response_model=ExpiryDateResponse)
async def get_expiry_date(token: str = Query(..., description="Token for which to fetch expiry dates")):
    # Filter the data based on the token
    token_data = scrip_master_data[scrip_master_data['token'] == str(token)]
    
    if token_data.empty:
        raise HTTPException(status_code=404, detail=f"Token {token} not found.")
    
    # Extract the name associated with the token
    name = token_data['name'].values[0]

    # Filter the options data related to the token (CE/PE options)
    option_data = scrip_master_data[
        (scrip_master_data['symbol'].str.contains(name, case=False, na=False)) &  # Match the name
        (scrip_master_data['symbol'].str.contains('CE|PE', case=False, na=False)) &  # Option type (Call/Put)
        (scrip_master_data['exch_seg'].isin(['NFO']))  # Valid exchange segments
    ]

    if option_data.empty:
        raise HTTPException(status_code=404, detail=f"No options data found for token {token}.")
    
    # Get the unique expiry dates for the filtered option data and sort them
    unique_expiries = option_data['expiry'].drop_duplicates().tolist()

    # Convert to datetime for sorting and then back to string format
    sorted_expiries = sorted(unique_expiries, key=lambda x: datetime.strptime(x, "%d%b%Y"))

    # Return the response with the sorted unique expiry dates
    return ExpiryDateResponse(status="SUCCESS", data=sorted_expiries)


