import json, os, sys, threading, logging
from datetime import datetime
from pathlib import Path
from flask import Flask, jsonify, request

FETCH_MACRO_DIR = Path(__file__).parent
sys.path.insert(0, str(FETCH_MACRO_DIR))
import fetch_macro
import fetch_space

MACRO_CACHE_FILE  = FETCH_MACRO_DIR / "output" / "macro_snapshot.json"
SPACE_CACHE_FILE  = FETCH_MACRO_DIR / "output" / "space_snapshot.json"
MACRO_MAX_AGE_H   = 6
SPACE_MAX_AGE_H   = 12
PORT = 5000
HOST = "127.0.0.1"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
app = Flask(__name__)
_macro_lock = threading.Lock()
_space_lock = threading.Lock()


def _load_json(path: Path):
    try:
        if path.exists():
            with open(path, encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        log.warning(f"Cache read failed ({path.name}): {e}")
    return None


def _cache_age_hours(snapshot) -> float:
    try:
        dt = datetime.fromisoformat(snapshot["meta"]["generated_at"])
        return (datetime.now() - dt).total_seconds() / 3600
    except Exception:
        return 999.0


def _run_macro_fetch():
    with _macro_lock:
        log.info("Running fetch_macro.run()...")
        snapshot = fetch_macro.run()
        MACRO_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(MACRO_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, ensure_ascii=False, indent=2)
        return snapshot


def _run_space_fetch(mode: str = "daily"):
    with _space_lock:
        log.info(f"Running fetch_space.run(mode={mode})...")
        snapshot = fetch_space.run(mode=mode)
        # fetch_space.run() already writes the file; return snapshot directly
        return snapshot


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.route("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.now().isoformat()})


@app.route("/macro")
def macro_fresh():
    try:
        snapshot = _run_macro_fetch()
        snapshot["_served_from"] = "fresh"
        return jsonify(snapshot)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/macro/cached")
def macro_cached():
    snapshot = _load_json(MACRO_CACHE_FILE)
    if snapshot is None or _cache_age_hours(snapshot) > MACRO_MAX_AGE_H:
        try:
            snapshot = _run_macro_fetch()
            snapshot["_served_from"] = "fresh"
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    else:
        snapshot["_served_from"] = f"cache ({_cache_age_hours(snapshot):.1f}h old)"
    return jsonify(snapshot)


@app.route("/space")
def space_fresh():
    mode = request.args.get("mode", "daily")
    try:
        snapshot = _run_space_fetch(mode=mode)
        snapshot["_served_from"] = "fresh"
        return jsonify(snapshot)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/space/cached")
def space_cached():
    mode = request.args.get("mode", "daily")
    snapshot = _load_json(SPACE_CACHE_FILE)
    if snapshot is None or _cache_age_hours(snapshot) > SPACE_MAX_AGE_H:
        try:
            snapshot = _run_space_fetch(mode=mode)
            snapshot["_served_from"] = "fresh"
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    else:
        snapshot["_served_from"] = f"cache ({_cache_age_hours(snapshot):.1f}h old)"
    return jsonify(snapshot)


if __name__ == "__main__":
    log.info(f"服务启动 -> http://{HOST}:{PORT}")
    app.run(host=HOST, port=PORT, debug=False, threaded=True)
