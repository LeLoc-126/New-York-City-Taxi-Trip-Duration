import requests
import pandas as pd
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import pickle

# Cache để tránh gọi lại API
route_cache = {}

# Đọc route_cache từ file .pkl nếu có
def load_cache():
    global route_cache
    try:
        with open('route_cache.pkl', 'rb') as f:
            route_cache = pickle.load(f)
    except FileNotFoundError:
        route_cache = {}

# Lưu route_cache vào file .pkl
def save_cache():
    with open('route_cache.pkl', 'wb') as f:
        pickle.dump(route_cache, f)

# Hàm gọi API và lấy quãng đường ngắn nhất
def get_shortest_osrm_route(pickup_lat, pickup_lon, dropoff_lat, dropoff_lon):
    key = (round(pickup_lat, 4), round(pickup_lon, 4), round(dropoff_lat, 4), round(dropoff_lon, 4))
    
    if key in route_cache:
        return route_cache[key]
    
    url = f"http://router.project-osrm.org/route/v1/driving/{pickup_lon},{pickup_lat};{dropoff_lon},{dropoff_lat}?alternatives=true&overview=false"
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if 'routes' in data and len(data['routes']) > 0:
                shortest = min(data['routes'], key=lambda x: x['distance'])
                result = {
                    'shortest_duration': shortest['duration'],
                    'shortest_distance': shortest['distance']
                }
                route_cache[key] = result
                time.sleep(1)  # tránh bị chặn
                return result
    except Exception as e:
        print("Lỗi khi gọi API OSRM:", e)
    
    return {'shortest_duration': None, 'shortest_distance': None}

# Hàm xử lý song song
def process_batch(batch_df):
    batch_results = []
    for _, row in batch_df.iterrows():
        result = get_shortest_osrm_route(
            row['pickup_latitude'], row['pickup_longitude'],
            row['dropoff_latitude'], row['dropoff_longitude']
        )
        batch_results.append(result)
    
    return batch_results

# Hàm xử lý song song với ThreadPoolExecutor
def parallel_processing(df, batch_size=1000):
    num_batches = len(df) // batch_size + 1
    all_results = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        
        # Dùng tqdm để hiện tiến trình của các batch
        with tqdm(total=num_batches, desc="Processing batches") as pbar:
            for batch_num in range(num_batches):
                batch_df = df[batch_num * batch_size: (batch_num + 1) * batch_size]
                future = executor.submit(process_batch, batch_df)
                futures.append(future)

            # Cập nhật tiến trình mỗi khi một future hoàn thành
            for future in as_completed(futures):
                all_results.extend(future.result())
                pbar.update(1)  # Cập nhật tiến trình khi hoàn thành một batch
    
    return all_results

# Lưu kết quả vào DataFrame
def update_dataframe(df, results):
    df[['shortest_duration', 'shortest_distance']] = pd.DataFrame(results)
    return df

# Main
load_cache()  # Đọc cache
df = pd.read_csv("/home/sonnd/trip_duration/nyc_taxi_with_shortest_route.csv")  # Đọc dữ liệu ban đầu (có cột pickup_latitude, pickup_longitude, dropoff_latitude, dropoff_longitude)

tqdm.pandas()

# Xử lý song song và hiện tiến trình
results = parallel_processing(df, batch_size=1000)

# Cập nhật DataFrame
df = update_dataframe(df, results)

# Lưu kết quả
df.to_csv("preprocess.csv", index=False)

# Lưu cache sau khi hoàn thành
save_cache()
