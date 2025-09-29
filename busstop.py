import json
import sys
import time
import os
from uuid import uuid4
from datetime import datetime, timezone
import boto3
from awscrt import auth, io, mqtt
from awsiot import mqtt_connection_builder
from shapely.geometry import Point, Polygon
import numpy as np
from sklearn.decomposition import PCA

# ====== 認証 ======
refresh_token = "eyJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.H39nGEQC70pdFWZwWNnz79PLbYLTdwtqXDJbiIiVDQDpa3RPgsp4FiWICZjMqVeEqnOBQbPUtSgtn48KKiLuvxuR8OhMtSDCdd2dtB4FUZv_DLTEJXBcabHGMxMdYyHxHJM-lNmELZBD9RbvgU9ULmQfyrhN__LnkFqjbYhffLimeSVjIv3cDjflU23QT7N6jJwqGsVZJ5NtcU912OoP7NvWKHdbNghZzH8k3iIhdGIjMeNc9iSsiIgftuGj7rekimbDIC1scQczmJDvwd7oVCVDO_nQ3v189kR1-JMTgQpIQfivJEcICsDrb3Ye5JbZV__JE5sMLgTWjP06-71o0w.vW6vy4fDFu0-MFsD.r_8kfdpp6qo05FfwbT1y4GTCt8u404SNWw4zz2xZEx_wH4M6LQ65EzabBclvetmXktWqdBG8sP5qinIyqTMZRRMI515Kmx1JsjAnwrPDtMAyvm2_Pke0FqS0gHf5bnl3jn7ueUl6d_OEpRZkat6CB5INd8-kd2sQAnjs2CoOoY3RMg8P-_2KPjhaFaxF8ZF1T7y0SfykJqnMxu8j5KVq4AfpLOGM3PivbysuTLHG-uD5obgsIchxpvOuzkzuyeZ_HyxxU4TCj0PuUoOaqfOw0vMkuelHKGvcppPu_HVfINS3N-BiHGwOOO22FUV5s5HW1nYIQX2GJsDStWqZLMKBvoPD0OWmj3D82EO1dJk7YxDgsDHAIb8ll3HS00SGSZjRmLTu0aDqYrRIRchkXIyc_pb12P0Wl-IvIKekDnmMJHtG61J0uMYh3PgVeoPI2OYDfg4hoU0t1iRUnbWS88eSOD6JHt2az7NfL3TVHfIAkKYLEy4P-BymJBObBaLFiNl9r0lUB1BonQ222qP6ixDRR-zpr6GNQOg6zdyHyLAZIAARpv8ZPjiws5KYJL1cecg9Y8vih2luSkkuD84R2KVKTSTQYpn7Fq8kOW9-r2EYrAhuSEqGzHcC4juSS07FdnfLMUHtCtxU_-eP8qLjsg7_Hm-ah_jEofgblIoqpZNx8V-jK-_w418ienI6moh8P0aUC3pR6QQZp7YKyM1Ri2Iai8WDiTWgT3ochgSyssRmzUSlrMqSgEFFqHGZBPWekZ9bSGNfSSWVBk1rfRGOXEQw8nXcW0AufeI8jpMgzF07obU6lqwJvbOI1MC_3U72vmifVtkheGzbapwDH68nngrCZUcihLILoyDf0GBQERYu7pOmpILw2AqDi_AyDQoP_a24OEx8XyC_LD7YRsh367fmY8MdkEn1HC3EAy4v0DgJdm-eNvVZUn8GABtZ1RSq4xfx_81GxrwDzmaLG4BAtMWkYERJOZMpzXIe74iyzr-98jHltrnIv6HimBK8LQGK4JaMoiNTd1r8MNvkMDrlDtAVVyuBemjmYNdtt7P7yBQYsT0VKLZZW33gIFFhJXgXCZFqi6bGk8hb7rpcDw4XJl78FyFa9I2OlcmxX71Czy6nJkXkuSoUD87P6gvVn23vq9eQZ6Ve76f6gLn01K-fM2noBZItDrouFKUFIyI4oTtUdazHwXt44hgNr-5LJeeUaO9C5P49h-cno8AqSDE6yGY6OZWlx3d2AazdqTY65V202kgF32FmhkyIHXaAFQMAnY7RPvaYltM9nEQkhYm4MVf_0uLtTEYTF0ABIc7TuGaEj_WRU3RcUU3aAuaPFQWM0ON0-ai6z43rmvfHmg.qoox94eh36fvGbOvIXpX4Q"
region = "ap-northeast-1"
user_pool_id = "ap-northeast-1_kRWuig6oV"
user_pool_client_id = "2jl8m0q968eudj7lubpdkuvq9v"
identity_pool_id = "ap-northeast-1:7e24baf3-0e4b-4c3a-bacf-ca1e9b7f4650"
endpoint = "ak6s01k4r928v-ats.iot.ap-northeast-1.amazonaws.com"
message_topic = "object/lidar/+/+"
client_id = "sample-" + str(uuid4())
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# ===== パラメータ =====
PERSISTENCE_SECONDS = 2.0 # オブジェクト情報保持時間
STOP_DURATION_SECONDS = 5.0 # バス停止判定時間
LOG_INTERVAL_SECONDS = 5.0 # ログ保存間隔

# ====== ポリゴン定義 (lon, lat) ======
polygons = {
    "honkan_shonandai": {
        "name": "２番のりば 湘南台駅西口 行き",
        "type": "boarding",
        "pedestrian": Polygon([
            (139.4282392,35.3875335),
            (139.4281976,35.387351),
            (139.428162,35.3872171),
            (139.4281409,35.3871479),
            (139.4281185,35.3870941),
            (139.4280735,35.3870279),
            (139.4280065,35.3869591),
            (139.4279488,35.3869082),
            (139.427867,35.3868568),
            (139.4277704,35.3868076),
            (139.4276813,35.3867672),
            (139.4277195,35.3867125),
            (139.4277906,35.3867486),
            (139.4278563,35.3867808),
            (139.4279273,35.3868218),
            (139.4279743,35.386853),
            (139.4280279,35.3868962),
            (139.4280816,35.3869448),
            (139.4281221,35.3869846),
            (139.4281536,35.3870212),
            (139.4281878,35.3870633),
            (139.4282144,35.3871076),
            (139.4282586,35.3872487),
            (139.4283311,35.3875193),
            (139.4282392,35.3875335)]),
        "bus_lane": Polygon([
            (139.428289,35.3875273),
            (139.4282085,35.3872239),
            (139.4282702,35.3872087),
            (139.428286,35.3872434),
            (139.4282977,35.3872803),
            (139.4283581,35.3875159),
            (139.428289,35.3875273)]),
        "origin": (139.428286, 35.387497)
    },
    "honkan_tsujido": {
        "name": "１番のりば 辻堂駅北口 行き",
        "type": "boarding",
        "pedestrian": Polygon([
            (139.4283333,35.3875482),
            (139.4284057,35.387815),
            (139.4283179,35.387827),
            (139.4282508,35.3875608),
            (139.4283333,35.3875482)]),
        "bus_lane": Polygon([
            (139.428293,35.3875537),
            (139.4283628,35.3875433),
            (139.4284332,35.3878095),
            (139.4283594,35.3878205),
            (139.428293,35.3875537)]),
        "origin": (139.4283, 35.3877)
    },
    "honkan_kosha": {
        "name": "おりば 中高等部 方面",
        "type": "disembarking",
        "pedestrian": Polygon([
            (139.4284165,35.3875998),
            (139.4285144,35.3875817),
            (139.4285922,35.3878797),
            (139.4284883,35.3878944),
            (139.4284165,35.3875998)]),
        "bus_lane": Polygon([
            (139.4284526,35.3878746),
            (139.4283816,35.3876063),
            (139.428446,35.3875943),
            (139.4285184,35.3878649),
            (139.4284526,35.3878746)   ])
    }
}

# ===== 時刻表JSONの読み込み =====
timetables = {}
try:
    with open("diagram_cal.json", "r", encoding="utf-8") as f:
        calendar_data = {str(item["daykey"]): str(item["wdid"]) for item in json.load(f)}
    with open("diagram_sfsh.json", "r", encoding="utf-8") as f:
        timetables["honkan_shonandai"] = json.load(f)
    with open("diagram_sfts.json", "r", encoding="utf-8") as f:
        timetables["honkan_tsujido"] = json.load(f)
    print("時刻表データを正常に読み込みました。")
except Exception as e:
    print(f"エラー: 時刻表ファイルが読み込めませんでした: {e}", file=sys.stderr)
    calendar_data = {}

# ===== 状態管理グローバル変数 =====
world_objects = {}
boarding_states = {
    "honkan_shonandai": {"status": "WAITING", "locked_on_bus": None, "locked_on_bus_id": None},
    "honkan_tsujido": {"status": "WAITING", "locked_on_bus": None, "locked_on_bus_id": None}
}
disembarking_state = {"status": "IDLE", "bus_id": None, "bus_arrival_time": None, "counting_start_time": None, "counted_person_ids": set(), "last_disembarked_count": 0, "last_arrival_time_str": None}

# ログ集約用の変数
log_buffer = {}
last_log_time = datetime.now(timezone.utc)

# ===== ユーティリティ & 解析関数 =====
def latlon_to_meters(lon, lat, origin_lon, origin_lat):
    lat_factor = 111320.0
    lon_factor = 111320.0 * np.cos(np.radians(origin_lat))
    return np.array([(lon - origin_lon) * lon_factor, (lat - origin_lat) * lat_factor])

def get_next_bus_info(now, stop_id):
    timetable_data = timetables.get(stop_id)
    if not calendar_data or not timetable_data:
        return {"status": "no_data"}
    today_key = now.strftime('%Y%m%d')
    wdid = calendar_data.get(today_key)
    if not wdid or wdid not in timetable_data or not timetable_data[wdid]:
        return {"status": "no_service"}
    todays_timetable = timetable_data[wdid]
    current_time_str = now.strftime('%H:%M')
    upcoming_buses = [bus for bus in todays_timetable if bus["time"].zfill(5) > current_time_str]
    if not upcoming_buses:
        return {"status": "ended"}
    next_bus = upcoming_buses[0]
    if len(upcoming_buses) == 1: next_bus["remark"] = "last"
    elif len(upcoming_buses) == 2: next_bus["remark"] = "second_to_last"
    else: next_bus["remark"] = None
    return {"status": "scheduled", **next_bus}

def analyze_boarding_stop(stop_id, active_objects, now):
    state = boarding_states[stop_id]
    poly = polygons[stop_id]
    
    persons = [
        obj for obj in active_objects 
        if obj.get("classification") in ["PERSON", "UNKNOWN"] 
        and poly["pedestrian"].contains(Point(obj["position"]["lon"], obj["position"]["lat"]))
    ]
    
    queue_count = len(persons)
    queue_length = 0.0
    if queue_count >= 2:
        person_coords = [[p["position"]["lon"], p["position"]["lat"]] for p in persons]
        pts = np.array([latlon_to_meters(lon, lat, *poly["origin"]) for lon, lat in person_coords])
        pca = PCA(n_components=2); pca.fit(pts)
        proj = pts @ pca.components_[0]
        queue_length = proj.max() - proj.min()
    
    is_stopped = False
    stopped_id = None
    vehicles = [obj for obj in active_objects if obj.get("classification") == "VEHICLE" and poly["bus_lane"].contains(Point(obj["position"]["lon"], obj["position"]["lat"]))]
    for v in vehicles:
        tracked = world_objects.get(v["id"])
        if tracked and tracked.get("stationary_start_time") and (now - tracked["stationary_start_time"]).total_seconds() >= STOP_DURATION_SECONDS:
            is_stopped = True; stopped_id = v["id"]; break

    next_bus = None
    if state["status"] == "WAITING":
        next_bus = get_next_bus_info(now, stop_id)
        if is_stopped and next_bus.get("status") == "scheduled":
            state["status"] = "LOCKED_ON"; state["locked_on_bus"] = next_bus; state["locked_on_bus_id"] = stopped_id
    elif state["status"] == "LOCKED_ON":
        next_bus = state["locked_on_bus"]
        bus_gone = not any(obj["id"] == state["locked_on_bus_id"] for obj in active_objects)
        if bus_gone:
            state["status"] = "WAITING"; state["locked_on_bus"] = None; state["locked_on_bus_id"] = None
            next_bus = get_next_bus_info(now, stop_id)
            
    return {
        "name": poly["name"],
        "type": poly["type"],
        "queue_count": queue_count, 
        "queue_length_meters": round(queue_length, 2), 
        "is_bus_stopped": is_stopped, 
        "next_bus": next_bus
    }

def analyze_disembarking_stop(active_objects, now):
    global disembarking_state
    state = disembarking_state
    poly = polygons["honkan_kosha"]
    
    if state["status"] == "IDLE":
        vehicles_in_lane = [obj for obj in active_objects if obj.get("classification") == "VEHICLE" and poly["bus_lane"].contains(Point(obj["position"]["lon"], obj["position"]["lat"]))]
        for bus in vehicles_in_lane:
            tracked_bus = world_objects.get(bus["id"])
            if tracked_bus and tracked_bus.get("stationary_start_time"):
                if (now - tracked_bus["stationary_start_time"]).total_seconds() >= STOP_DURATION_SECONDS:
                    state["status"] = "BUS_STOPPED"; state["bus_id"] = bus["id"]; state["bus_arrival_time"] = now
                    print(f"降車バス停: バス停車を検知 (ID: {bus['id']})")
                    break
    elif state["status"] == "BUS_STOPPED":
        bus_still_here = any(obj["id"] == state["bus_id"] for obj in active_objects)
        if not bus_still_here:
            state["status"] = "COUNTING"; state["counting_start_time"] = now; state["counted_person_ids"] = set()
            print(f"降車バス停: バス出発、降車カウント開始")
    elif state["status"] == "COUNTING":
        if (now - state["counting_start_time"]).total_seconds() <= 5: # 5秒カウント
            people_in_area = {
                obj["id"] for obj in active_objects 
                if obj.get("classification") in ["PERSON", "UNKNOWN"] 
                and poly["pedestrian"].contains(Point(obj["position"]["lon"], obj["position"]["lat"]))
            }
            state["counted_person_ids"].update(people_in_area)
        else:
            count = len(state["counted_person_ids"]); state["last_disembarked_count"] = count; state["last_arrival_time_str"] = state["bus_arrival_time"].strftime('%H:%M')
            print(f"降車バス停: カウント終了。降車人数: {count}人")
            state["status"] = "IDLE"; state["bus_id"] = None
            
    return {
        "name": poly["name"],
        "type": poly["type"],
        "is_bus_stopped": state["status"] == "BUS_STOPPED", 
        "last_disembarked_count": state["last_disembarked_count"], 
        "last_arrival_time_str": state["last_arrival_time_str"]
    }
# ===== JSON 保存系 =====
def save_snapshot(snapshot, path="busstop_snapshot.json"):
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f: json.dump(snapshot, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)
def append_log(snapshot, path="busstop_log.jsonl"):
    line = json.dumps({"timestamp": datetime.now().isoformat(), **snapshot}, ensure_ascii=False)
    with open(path, "a", encoding="utf-8") as f: f.write(line + "\n")

# ===== AWS認証 & MQTT通信 =====
def fetch_id_token(refresh_token: str, user_pool_client_id: str, region: str) -> str:
    client = boto3.client("cognito-idp", region_name=region)
    response = client.initiate_auth(AuthFlow='REFRESH_TOKEN_AUTH', AuthParameters={'REFRESH_TOKEN': refresh_token}, ClientId=user_pool_client_id)
    return response['AuthenticationResult']['IdToken']
def fetch_identity_id(id_token: str, user_pool_id: str, identity_pool_id: str, region: str) -> str:
    client = boto3.client("cognito-identity", region_name=region)
    response = client.get_id(IdentityPoolId=identity_pool_id, Logins={f"cognito-idp.{region}.amazonaws.com/{user_pool_id}": id_token})
    return response['IdentityId']

def on_message_received(topic: str, payload: bytes, **kwargs):
    global world_objects, last_log_time, log_buffer
    decoded_payload = json.loads(payload)
    timestamp_str = decoded_payload.get("latest_timestamp")
    now = datetime.fromisoformat(timestamp_str) if timestamp_str else datetime.now(timezone.utc)
    
    # --- 1. ワールドモデルの更新 ---
    for obj in decoded_payload.get("objects", []):
        obj_id = obj["id"]
        if obj_id not in world_objects:
            world_objects[obj_id] = {"data": obj, "last_seen": now, "stationary_start_time": now}
        else:
            prev_pos = world_objects[obj_id]["data"]["position"]
            current_pos = obj["position"]
            if abs(prev_pos['lon'] - current_pos['lon']) > 1e-5 or abs(prev_pos['lat'] - current_pos['lat']) > 1e-5:
                world_objects[obj_id]["stationary_start_time"] = now
            world_objects[obj_id]["data"] = obj
            world_objects[obj_id]["last_seen"] = now

    # --- 2. 古いオブジェクトの消去 ---
    expired_ids = [obj_id for obj_id, data in world_objects.items() if (now - data["last_seen"]).total_seconds() > PERSISTENCE_SECONDS]
    for obj_id in expired_ids:
        del world_objects[obj_id]

    # --- 3. 安定化されたデータで解析 ---
    active_objects = [data["data"] for data in world_objects.values()]
    stop_results = {}
    stop_results["honkan_shonandai"] = analyze_boarding_stop("honkan_shonandai", active_objects, now)
    stop_results["honkan_tsujido"] = analyze_boarding_stop("honkan_tsujido", active_objects, now)
    stop_results["honkan_kosha"] = analyze_disembarking_stop(active_objects, now)

    gbfs_info = None
    try:
        with open("gbfs_status.json", "r", encoding="utf-8") as f:
            gbfs_info = json.load(f)
    except Exception:
        pass
    
    # --- 4. スナップショットJSON出力（毎秒更新）---
    snapshot_output = {
        "timestamp": timestamp_str,
        "stops": stop_results,
        "gbfs": gbfs_info,
        "raw": {"objects": active_objects}
    }
    save_snapshot(snapshot_output)
    
    # ログの集約と定期書き込み
    # 1. バッファを最新のオブジェクトで更新
    for obj in active_objects:
        log_buffer[obj["id"]] = obj

    # 2. 前の書き込みから指数経ったかチェック
    if (now - last_log_time).total_seconds() >= LOG_INTERVAL_SECONDS:
        print(f"--- ログ書き込み (間隔: {LOG_INTERVAL_SECONDS}秒) ---")
        log_output = {
            "timestamp": timestamp_str,
            "stops": stop_results,
            "raw": {"objects": list(log_buffer.values())} # バッファをリスト化
        }
        append_log(log_output)
        
        # タイマーリセット
        log_buffer = {}
        last_log_time = now
    
    print(f"\r[{timestamp_str}] Active: {len(active_objects)}", end="")

def connect_and_subscribe():
    id_token = fetch_id_token(refresh_token, user_pool_client_id, region)
    global identity_id
    if identity_id is None:
        identity_id = fetch_identity_id(id_token, user_pool_id, identity_pool_id, region)
    credentials_provider = auth.AwsCredentialsProvider.new_cognito(endpoint=f"cognito-identity.{region}.amazonaws.com", identity=identity_id, tls_ctx=io.ClientTlsContext(io.TlsContextOptions()), logins=[(f"cognito-idp.{region}.amazonaws.com/{user_pool_id}", id_token)])
    mqtt_connection = mqtt_connection_builder.websockets_with_default_aws_signing(endpoint=endpoint, client_id=client_id, region=region, credentials_provider=credentials_provider, clean_session=False, reconnect_min_timeout_secs=1, keep_alive_secs=30)
    connect_future = mqtt_connection.connect()
    connect_future.result()
    print("Connected!")
    subscribe_future, _ = mqtt_connection.subscribe(topic=message_topic, qos=mqtt.QoS.AT_MOST_ONCE, callback=on_message_received)
    subscribe_future.result()
    print(f"Subscribed to {message_topic}")
    while True:
        time.sleep(1)
def main():
    global identity_id; identity_id = None; backoff_time = 1
    while True:
        try: connect_and_subscribe()
        except Exception as e: print(e, file=sys.stderr)
        time.sleep(backoff_time); backoff_time = min(backoff_time * 2, 600)
if __name__ == "__main__":
    main()