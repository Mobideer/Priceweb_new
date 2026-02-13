#!/bin/bash

# --- Configuration ---
# Absolute path to the project directory
PROJECT_DIR="/opt/priceweb_new"

# Telegram Notification Settings
export TG_BOT_TOKEN="ВАШ_ТОКЕН_БОТА"
export TG_CHAT_ID="ВАШ_CHAT_ID"

# Database path (matching the web app)
export PRICE_DB_PATH="${PROJECT_DIR}/data/priceweb.sqlite"

# --- Execution ---
cd "$PROJECT_DIR"
./venv/bin/python worker.py
