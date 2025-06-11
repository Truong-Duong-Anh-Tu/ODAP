# exchange_rate.py

import requests
from datetime import datetime
import time

# Biến toàn cục
current_exchange_rate = 24000.0
last_update = None

def fetch_exchange_rate():
    """
    Lấy tỷ giá USD/VND từ ExchangeRate-API hoặc từ exchangerate.host
    """
    global current_exchange_rate, last_update

    try:
        # Option 1: ExchangeRate-API
        url = "https://api.exchangerate-api.com/v4/latest/USD"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            current_exchange_rate = float(data['rates'].get('VND', 24000))
            last_update = datetime.now()
            print(f"[API 1] 1 USD = {current_exchange_rate} VND lúc {last_update}")
            return True
    except Exception as e:
        print(f"Lỗi API chính: {e}")

    try:
        # Option 2: exchangerate.host
        url = "https://api.exchangerate.host/latest?base=USD&symbols=VND"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            current_exchange_rate = float(data['rates'].get('VND', 24000))
            last_update = datetime.now()
            print(f"[API dự phòng] 1 USD = {current_exchange_rate} VND lúc {last_update}")
            return True
    except Exception as e:
        print(f"Lỗi API dự phòng: {e}")

    return False

def update_exchange_rate_periodically():
    """
    Cập nhật tỷ giá định kỳ mỗi 4 tiếng
    """
    while True:
        fetch_exchange_rate()
        time.sleep(14400)  # 4 tiếng = 14400 giây
