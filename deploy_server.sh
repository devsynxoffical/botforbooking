#!/usr/bin/env bash
# ==============================================================================
# Automated Server Setup & 24/7 Service Installer for Cita Previa Monitor Bot
# Supports Ubuntu 20.04+, 22.04+, Debian 11/12
# ==============================================================================
set -e

INSTALL_DIR="/opt/cita-bot"

echo "======================================================================"
echo " 🚀 Installing Cita Previa 24/7 Monitor & Telegram Broadcaster"
echo "======================================================================"

# Check root
if [ "$EUID" -ne 0 ]; then
  echo "Please run as root (or use sudo ./deploy_server.sh)"
  exit 1
fi

echo "[1/6] Updating system packages..."
apt-get update -y
apt-get install -y python3 python3-pip python3-venv curl git build-essential

echo "[2/6] Preparing installation directory at $INSTALL_DIR..."
mkdir -p "$INSTALL_DIR"
cp -r . "$INSTALL_DIR/"
cd "$INSTALL_DIR"

echo "[3/6] Setting up Python virtual environment..."
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
fi
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt

echo "[4/6] Installing Playwright browser dependencies..."
.venv/bin/playwright install chromium
.venv/bin/playwright install-deps chromium || true

echo "[5/6] Installing systemd background services..."
cp systemd/cita-monitor.service /etc/systemd/system/cita-monitor.service
cp systemd/cita-queue.service /etc/systemd/system/cita-queue.service

systemctl daemon-reload
systemctl enable cita-monitor.service
systemctl enable cita-queue.service

echo "[6/6] Starting 24/7 background services..."
systemctl restart cita-queue.service
systemctl restart cita-monitor.service

echo ""
echo "======================================================================"
echo " 🎉 INSTALLATION COMPLETE & RUNNING 24/7!"
echo "======================================================================"
echo " - Admin & Queue Board: http://$(curl -s ifconfig.me):8000/admin"
echo " - Monitor logs:        journalctl -u cita-monitor -f"
echo " - Queue server logs:   journalctl -u cita-queue -f"
echo ""
echo "To manage services:"
echo "   systemctl restart cita-monitor"
echo "   systemctl stop cita-monitor"
echo "   systemctl status cita-monitor"
echo "======================================================================"
