from elasticsearch import Elasticsearch
import requests
from datetime import datetime
import schedule
import time
import json

# Kết nối Elasticsearch (thay đổi thông tin nếu cần)
es = Elasticsearch(
    ['http://localhost:9200'],  # Hoặc URL Elasticsearch của bạn
    # basic_auth=("username", "password")  # Nếu có authentication
)

# Test connection
try:
    if es.ping():
        print("✅ Kết nối Elasticsearch thành công!")
    else:
        print("❌ Không thể kết nối Elasticsearch")
        exit()
except Exception as e:
    print(f"❌ Lỗi kết nối: {e}")
    exit()

def fetch_and_index_data():
    """Fetch dữ liệu từ API và index vào Elasticsearch"""
    try:
        # ==== THAY ĐỔI URL API CỦA BẠN Ở ĐÂY ====
        api_url = "https://api.example.com/your-endpoint"
        
        print(f"\n⏰ [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Đang fetch dữ liệu từ API...")
        
        # Gọi API
        response = requests.get(
            api_url,
            timeout=30,
            # headers={"Authorization": "Bearer YOUR_TOKEN"}  # Nếu cần auth
        )
        
        if response.status_code == 200:
            data = response.json()
            
            # Xử lý dữ liệu
            # Nếu API trả về array, loop qua từng item
            if isinstance(data, list):
                for item in data:
                    item['@timestamp'] = datetime.utcnow().isoformat()
                    item['indexed_at'] = datetime.now().isoformat()
                    
                    # Index vào Elasticsearch
                    index_name = f"api-logs-{datetime.now().strftime('%Y.%m.%d')}"
                    result = es.index(index=index_name, document=item)
                    print(f"  ✅ Indexed doc ID: {result['_id']}")
            
            # Nếu API trả về object
            else:
                data['@timestamp'] = datetime.utcnow().isoformat()
                data['indexed_at'] = datetime.now().isoformat()
                
                index_name = f"api-logs-{datetime.now().strftime('%Y.%m.%d')}"
                result = es.index(index=index_name, document=data)
                print(f"  ✅ Indexed doc ID: {result['_id']} vào {index_name}")
            
            print(f"✅ Hoàn thành!")
            return True
            
        else:
            print(f"❌ API trả về status code: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Lỗi khi gọi API: {e}")
        return False
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        return False

# Chạy ngay lần đầu
print("🚀 Khởi động API Indexer...")
fetch_and_index_data()

# Schedule chạy định kỳ mỗi 5 phút
schedule.every(5).minutes.do(fetch_and_index_data)

print("\n⏰ Scheduler đang chạy - fetch API mỗi 5 phút")
print("   Nhấn Ctrl+C để dừng\n")

# Main loop
try:
    while True:
        schedule.run_pending()
        time.sleep(1)
except KeyboardInterrupt:
    print("\n👋 Đã dừng script!")