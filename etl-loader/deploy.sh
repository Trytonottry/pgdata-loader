#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="/opt/etl_loader"
VENV_DIR="$PROJECT_DIR/venv"
SERVICE_USER="etluser"
SERVICE_GROUP="etluser"
ETL_SERVICE="etl-loader.service"
MON_SERVICE="etl-monitor.service"

echo "=== ETL one-click deploy ==="

# 1) System deps
echo "[1/6] Installing system packages..."
apt update
apt install -y python3 python3-venv python3-pip build-essential libpq-dev \
  libffi-dev libssl-dev zlib1g-dev libxml2-dev libxslt1-dev postgresql-client \
  jq gzip

# 2) Create project dir and user
echo "[2/6] Creating project directory and user..."
mkdir -p "$PROJECT_DIR"
groupadd -f "$SERVICE_GROUP" || true
id -u "$SERVICE_USER" &>/dev/null || useradd -r -s /usr/sbin/nologin -g "$SERVICE_GROUP" -d "$PROJECT_DIR" "$SERVICE_USER"
chown -R "$SERVICE_USER":"$SERVICE_GROUP" "$PROJECT_DIR"
chmod 755 "$PROJECT_DIR"

# 3) Create venv & install pip packages
echo "[3/6] Creating venv and installing Python packages..."
python3 -m venv "$VENV_DIR"
"$VENV_DIR/bin/pip" install --upgrade pip
"$VENV_DIR/bin/pip" install pandas psycopg2-binary orjson tqdm fastapi uvicorn[standard] prometheus-client openpyxl pyarrow fastparquet python-magic

# 4) Create folders
echo "[4/6] Creating folders..."
mkdir -p "$PROJECT_DIR/logs"
mkdir -p "$PROJECT_DIR/tmp"
mkdir -p "$PROJECT_DIR/utils"
chown -R "$SERVICE_USER":"$SERVICE_GROUP" "$PROJECT_DIR"

# 5) Write config.yaml
echo "[5/6] Writing config.yaml..."
cat > "$PROJECT_DIR/config.yaml" <<'YAML'
# ETL config
data_dir: /opt/data
pg_url: postgresql+psycopg2://postgres:password@127.0.0.1:5432/bigdata
workers: 6
batch_size: 5000
use_copy: true
tmp_dir: /opt/etl_loader/tmp
monitor_host: 0.0.0.0
monitor_port: 8080
log_file: /opt/etl_loader/logs/etl.log
error_log: /opt/etl_loader/logs/errors.log
YAML

chown "$SERVICE_USER":"$SERVICE_GROUP" "$PROJECT_DIR/config.yaml"
chmod 640 "$PROJECT_DIR/config.yaml"

# 6) Deploy code files (etl.py, monitor_api.py, utils)
echo "[6/6] Deploying code files..."
# etl.py
cat > "$PROJECT_DIR/etl.py" <<'PY'
# (ETL code placed here by deploy script)
# To keep deploy.sh readable, actual ETL code will be copied next step by manual step or user-run rsc.
PY

# monitor_api.py
cat > "$PROJECT_DIR/monitor_api.py" <<'PY'
# (Monitor API placeholder)
PY

# utils placeholder - actual content will be created by subsequent commands or by manual copy.
# For convenience, user can overwrite /opt/etl_loader/etl.py and monitor_api.py with the full content provided separately.

systemctl daemon-reload

echo "=== Deploy done ==="
echo "Now: edit $PROJECT_DIR/config.yaml to set real PG credentials & data_dir."
echo "Place ETL code into $PROJECT_DIR/etl.py and monitor into $PROJECT_DIR/monitor_api.py (already placeholders)."
echo "Create systemd units if you want automatic start (I can generate them on demand)."
