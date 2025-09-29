from flask import Flask, jsonify, render_template
import json
import os

app = Flask(__name__)

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
