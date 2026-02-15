#!/bin/bash
set -e

# --- Configuration ---
PROJECT_DIR="/opt/priceweb_new"

# Telegram Notification Settings
export TG_BOT_TOKEN="ВАШ_ТОКЕН_БОТА"
export TG_CHAT_ID="ВАШ_CHAT_ID"
export TZ="Europe/Moscow"
export PRICE_DB_PATH="${PROJECT_DIR}/data/priceweb.db"

# --- Execution ---
echo "Starting worker at $(date)"
cd "$PROJECT_DIR"

if [ ! -f "worker.py" ]; then
    echo "Error: worker.py not found in $PROJECT_DIR"
    exit 1
fi

./venv/bin/python worker.py
echo "Finished at $(date)"
