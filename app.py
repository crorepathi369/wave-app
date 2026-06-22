"""
╔══════════════════════════════════════════════════════════════════════╗
║           WAVE — Flask Backend  (Render deployment)                 ║
╚══════════════════════════════════════════════════════════════════════╝

Endpoints:
  GET  /api/health                      ← keep-alive ping
  GET  /api/yf_cache/<filename>         ← serve scan cache JSON
  GET  /api/wave_hist/<filename>        ← serve backfill history JSON
  GET  /api/list/yf_cache               ← list available cache files
  GET  /api/list/wave_hist              ← list available hist files

On startup: restores data branch from GitHub into local DATA_DIR.
"""

import os, json, subprocess, logging
from pathlib import Path
from flask import Flask, jsonify, send_file, abort
from flask_cors import CORS

# ── Config ────────────────────────────────────────────────────────────
DATA_DIR     = Path(os.environ.get("DATA_DIR", "/opt/render/project/src/data"))
YF_CACHE_DIR = DATA_DIR / "yf_cache"
WAVE_HIST_DIR= DATA_DIR / "wave_hist"

GITHUB_REPO  = os.environ.get("GITHUB_REPO", "")   # e.g. "crorepathi369/wave-app"
DATA_BRANCH  = os.environ.get("DATA_BRANCH", "data")
GH_TOKEN     = os.environ.get("GH_TOKEN", "")       # GitHub PAT with repo read access

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("wave")

app = Flask(__name__)
CORS(app, origins="*")   # GitHub Pages origin; tighten if needed


# ══════════════════════════════════════════════════════════════════════
# Data restore on startup
# ══════════════════════════════════════════════════════════════════════
def restore_data():
    """
    Pull the data branch from GitHub into DATA_DIR.
    Mirrors the TradeEdge pattern exactly.
    """
    if not GITHUB_REPO or not GH_TOKEN:
        log.warning("GITHUB_REPO or GH_TOKEN not set — skipping data restore")
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    YF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    WAVE_HIST_DIR.mkdir(parents=True, exist_ok=True)

    repo_url = f"https://{GH_TOKEN}@github.com/{GITHUB_REPO}.git"
    tmp_dir  = Path("/tmp/wave_data_branch")

    try:
        if tmp_dir.exists():
            subprocess.run(["rm", "-rf", str(tmp_dir)], check=True)

        log.info(f"Cloning data branch from {GITHUB_REPO}...")
        subprocess.run([
            "git", "clone",
            "--depth", "1",
            "--branch", DATA_BRANCH,
            repo_url,
            str(tmp_dir)
        ], check=True, capture_output=True)

        # Copy yf_cache/
        src_yf = tmp_dir / "yf_cache"
        if src_yf.exists():
            for f in src_yf.glob("*.json"):
                dest = YF_CACHE_DIR / f.name
                dest.write_bytes(f.read_bytes())
            log.info(f"Restored {len(list(src_yf.glob('*.json')))} yf_cache files")

        # Copy wave_hist/
        src_hist = tmp_dir / "wave_hist"
        if src_hist.exists():
            for f in src_hist.glob("*.json"):
                dest = WAVE_HIST_DIR / f.name
                dest.write_bytes(f.read_bytes())
            log.info(f"Restored {len(list(src_hist.glob('*.json')))} wave_hist files")

        subprocess.run(["rm", "-rf", str(tmp_dir)], check=True)
        log.info("Data restore complete")

    except subprocess.CalledProcessError as e:
        log.error(f"Data restore failed: {e.stderr.decode() if e.stderr else e}")
    except Exception as e:
        log.error(f"Data restore error: {e}")


restore_data()


# ══════════════════════════════════════════════════════════════════════
# Routes
# ══════════════════════════════════════════════════════════════════════

@app.route("/api/health")
def health():
    yf_count   = len(list(YF_CACHE_DIR.glob("*.json")))  if YF_CACHE_DIR.exists()   else 0
    hist_count = len(list(WAVE_HIST_DIR.glob("*.json"))) if WAVE_HIST_DIR.exists() else 0
    return jsonify({
        "status": "ok",
        "yf_cache_files": yf_count,
        "wave_hist_files": hist_count,
    })


@app.route("/api/yf_cache/<filename>")
def get_yf_cache(filename):
    """
    Serve a single yf_cache JSON file.
    WAVE requests these as: /api/yf_cache/<safe_key>.json
    """
    # Basic safety: only allow .json, no path traversal
    if not filename.endswith(".json") or "/" in filename or ".." in filename:
        abort(400)
    path = YF_CACHE_DIR / filename
    if not path.exists():
        abort(404)
    return send_file(path, mimetype="application/json")


@app.route("/api/wave_hist/<filename>")
def get_wave_hist(filename):
    """
    Serve a single wave_hist JSON file.
    WAVE requests these as: /api/wave_hist/<SYMBOL>.json
    """
    if not filename.endswith(".json") or "/" in filename or ".." in filename:
        abort(400)
    path = WAVE_HIST_DIR / filename
    if not path.exists():
        abort(404)
    return send_file(path, mimetype="application/json")


@app.route("/api/list/yf_cache")
def list_yf_cache():
    """List all available yf_cache filenames (for debug / warm-all)."""
    if not YF_CACHE_DIR.exists():
        return jsonify([])
    files = sorted(f.name for f in YF_CACHE_DIR.glob("*.json"))
    return jsonify(files)


@app.route("/api/list/wave_hist")
def list_wave_hist():
    """List all available wave_hist filenames."""
    if not WAVE_HIST_DIR.exists():
        return jsonify([])
    files = sorted(f.name for f in WAVE_HIST_DIR.glob("*.json"))
    return jsonify(files)


# ── Dev entry point ───────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=True, port=5001)
