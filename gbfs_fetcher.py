import requests
import json
import time
from datetime import datetime
import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# ===== 設定項目 =====

GBFS_AUTO_DISCOVERY_URL = "https://api-public.odpt.org/api/v4/gbfs/hellocycling/gbfs.json" 

# ★監視したい駐輪場のIDを入れてね
TARGET_STATION_ID = "5143"

# 情報を更新する間隔（秒）
FETCH_INTERVAL_SECONDS = 30

def save_status(data, path="gbfs_status.json"):
    """ステータスをJSONファイルにアトミックに保存する"""
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)

def main():
    station_status_url = None
    
    # 最初にgbfs.jsonを読んで、station_status.jsonのURLを見つける
    try:
        print("GBFSフィード情報を取得中...")
        response = requests.get(GBFS_AUTO_DISCOVERY_URL)
        response.raise_for_status()
        gbfs_info = response.json()
        
        for feed in gbfs_info["data"]["ja"]["feeds"]:
            if feed["name"] == "station_status":
                station_status_url = feed["url"]
                break
        
        if not station_status_url:
            print("エラー: station_status のURLが見つかりませんでした。")
            return
            
        print(f"ステータスURLを発見: {station_status_url}")

    except Exception as e:
        print(f"GBFSの初期化エラー: {e}")
        return

    # ここから無限ループで情報を取得し続ける
    while True:
        try:
            print(f"駐輪場ID: {TARGET_STATION_ID} の情報を取得中...")
            response = requests.get(station_status_url)
            response.raise_for_status()
            status_data = response.json()
            
            found_station = None
            for station in status_data["data"]["stations"]:
                if station["station_id"] == TARGET_STATION_ID:
                    found_station = station
                    break
            
            if found_station:
                # 合計の枠数 = 利用可能な自転車 + 空きのドック
                total_docks = found_station.get("num_bikes_available", 0) + found_station.get("num_docks_available", 0)
                
                output = {
                    "timestamp": datetime.now().isoformat(),
                    "bikes_available": found_station.get("num_bikes_available"),
                    "docks_available": found_station.get("num_docks_available"),
                    "total_docks": total_docks
                }
                save_status(output)
                print(f"更新完了: {output['bikes_available']} / {output['total_docks']} 台が利用可能")
            else:
                print(f"エラー: 駐輪場ID {TARGET_STATION_ID} が見つかりませんでした。")

        except Exception as e:
            print(f"情報取得中にエラーが発生しました: {e}")
        
        time.sleep(FETCH_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()