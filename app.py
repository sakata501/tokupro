from flask import Flask, jsonify, render_template
import json, os, threading, subprocess

app = Flask(__name__)

# --- busstop.py をバックグラウンドで起動 ---
def run_busstop():
    subprocess.Popen(["python", "busstop.py"])

# Flask 起動時に busstop.py を走らせる
threading.Thread(target=run_busstop, daemon=True).start()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/snapshot.json")
def snapshot():
    if os.path.exists("busstop_snapshot.json"):
        with open("busstop_snapshot.json", "r") as f:
            data = json.load(f)
    else:
        data = {"status": "no data yet"}
    return jsonify(data)
