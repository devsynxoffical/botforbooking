#!/usr/bin/env bash
# Runner for Cita Previa Extranjería Live Monitor & Telegram Broadcaster Daemon
cd "$(dirname "$0")"

if [ ! -d ".venv" ]; then
    echo "Virtual environment not found."
    exit 1
fi

echo "=================================================================="
echo " 📡 Starting Live Appointment Monitor & Telegram Broadcaster"
echo "=================================================================="
echo "Monitored targets and intervals are read from orders.db & config.json."
echo "Press Ctrl+C to stop the monitor daemon."
echo ""

.venv/bin/python monitor.py --daemon

