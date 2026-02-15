#!/bin/bash
# Helper script to restart all PriceWeb services after an update

echo "🔄 Restarting PriceWeb services..."

echo "🔄 Reloading systemd daemon..."
sudo systemctl daemon-reload

# Web App
echo "  - Restarting Web App (port 5002)..."
sudo systemctl restart priceweb-web.service

# Telegram Bot
echo "  - Restarting Telegram Bot..."
sudo systemctl restart priceweb-bot.service

# Worker
echo "  - Restarting Worker Service..."
sudo systemctl restart priceweb-worker.service

echo "--------------------------------------------------"
echo "✅ All services restarted."
echo "💡 To see if everything is OK, run:"
echo "   systemctl status priceweb-web priceweb-bot priceweb-worker"
echo "--------------------------------------------------"
