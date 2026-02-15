#!/bin/bash
# Helper script to restart all PriceWeb services after an update

echo "🔄 Restarting PriceWeb services..."

echo "🔄 1. Stopping all services..."
sudo systemctl stop priceweb-web.service priceweb-bot.service priceweb-worker.service

echo "🔄 2. Killing any stray Python processes (just in case)..."
sudo pkill -f "app:app" || true
sudo pkill -f "tg_bot.py" || true
sudo pkill -f "worker.py" || true

echo "🔄 3. Reloading systemd daemon..."
sudo systemctl daemon-reload

# Web App
echo "🔄 4. Starting Web App (port 5002)..."
sudo systemctl start priceweb-web.service

# Telegram Bot
echo "🔄 5. Starting Telegram Bot..."
sudo systemctl start priceweb-bot.service

# Worker
echo "🔄 6. Starting Worker Service..."
sudo systemctl start priceweb-worker.service

echo "--------------------------------------------------"
echo "✅ Restart complete. Checking status..."
sleep 2
systemctl status priceweb-web priceweb-bot --no-pager
echo "--------------------------------------------------"
echo "💡 If status above is 'active', you're good to go!"
echo "💡 Use '/menu' in bot and then '🔍 Диагностика' to verify."
echo "--------------------------------------------------"
