#!/usr/bin/env bash
set -e

PROJECT_DIR="/opt/etl_loader"
SERVICE_NAME="etl-loader.service"

echo "=== ETL Deploy Script Started ==="

# --- Install system dependencies ---
echo "[1/6] Installing system packages..."
sudo apt update
sudo apt install -y \
    python3 python3-venv python3-pip \
    build-essential libpq-dev \
    libffi-dev libssl-dev \
    libxml2-dev libxslt1-dev \
    zlib1g-dev libjpeg-dev libfreetype6-dev \
    postgresql-client gzip

# --- Prepare project directory ---
echo "[2/6] Creating project directory..."
sudo mkdir -p $PROJECT_DIR
sudo chmod 777 $PROJECT_DIR

# --- Create virtual environment ---
echo "[3/6] Creating Python venv..."
python3 -m venv $PROJECT_DIR/venv
source $PROJECT_DIR/venv/bin/activate

# --- Install Python dependencies ---
echo "[4/6] Installing Python dependencies..."
$PROJECT_DIR/venv/bin/pip install --upgrade pip
$PROJECT_DIR/venv/bin/pip install \
    psycopg2-binary \
    pandas \
    openpyxl \
    xlrd \
    orjson \
    tqdm \
    python-dateutil

# --- Place ETL script ---
echo "[5/6] Generating ETL script..."
cat << 'EOF' > $PROJECT_DIR/etl.py
# PLACEHOLDER:
# сюда ты вставишь основной ETL-код,
# тот самый "многопроцессный конвейер".
print("ETL loader installed correctly.")
EOF

# --- Create systemd service ---
echo "[6/6] Creating systemd service..."

sudo bash -c "cat << EOF > /etc/systemd/system/$SERVICE_NAME
[Unit]
Description=ETL Loader Service
After=network.target

[Service]
Type=simple
WorkingDirectory=$PROJECT_DIR
ExecStart=$PROJECT_DIR/venv/bin/python3 $PROJECT_DIR/etl.py
Restart=on-failure
User=root
Group=root

[Install]
WantedBy=multi-user.target
EOF"

sudo systemctl daemon-reload
sudo systemctl enable $SERVICE_NAME

echo "=== Deploy Complete! ==="
echo "Запуск: sudo systemctl start $SERVICE_NAME"
echo "Логи:   journalctl -u $SERVICE_NAME -f"
