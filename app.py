from flask import Flask, jsonify, render_template
import json
import os

app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/snapshot.json")
def snapshot():
    """最新のバス停スナップショットを返すエンドポイント"""
    filepath = "busstop_snapshot.json"
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            data = {"status": "error", "message": str(e)}
    else:
        data = {"status": "no data yet"}
    return jsonify(data)


if __name__ == "__main__":
    # ローカルデバッグ用
    app.run(host="0.0.0.0", port=5000, debug=True)
