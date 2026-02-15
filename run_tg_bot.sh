#!/bin/bash

# Navigate to the project directory
cd "$(dirname "$0")"

# Start the bot using the virtual environment
echo "Starting PriceWeb New Telegram Bot..."
./venv/bin/python3 tg_bot.py
