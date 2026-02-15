#!/bin/bash
# Helper script to restart all PriceWeb services after an update

echo "🔄 Restarting PriceWeb services..."

# Web App
echo "  - Restarting Web App (port 5002)..."
sudo systemctl restart priceweb-web.service

# Telegram Bot
echo "  - Restarting Telegram Bot..."
sudo systemctl restart priceweb-bot.service

# Worker (usually simple type, but good to restart)
echo "  - Restarting Worker..."
sudo systemctl restart priceweb-worker.service

echo "✅ All services restarted. You can check status with: systemctl status priceweb-*"
