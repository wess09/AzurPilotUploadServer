#!/bin/sh
set -eu
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

# Capture Compose output: configuration values can include service credentials.
python3 - <<'PY'
import json
import subprocess
import sys

config = subprocess.run(["docker", "compose", "config", "--format", "json"], capture_output=True, text=True)
if config.returncode:
    sys.exit("Compose configuration is invalid; inspect it locally before starting.")
environment = json.loads(config.stdout)["services"]["uploader"]["environment"]
required = ("PAN_CLIENT_ID", "PAN_CLIENT_SECRET", "WORKER_TOKEN")
if environment.get("WORKER_MODE", "reverse") == "forward":
    required += ("WORKER_WS_URL",)
missing = [name for name in required if not environment.get(name)]
if missing:
    sys.exit("Configure these fields in .env before accepting uploads: " + ", ".join(missing))
PY

exec docker compose up -d --no-build --pull never
