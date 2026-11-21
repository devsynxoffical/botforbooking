#!/usr/bin/env bash
# Stop Background Full Auto Booker

PID_FILE="full_auto.pid"

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "🛑 Stopping Full Auto Booker (PID: $PID)..."
        kill "$PID"
        rm -f "$PID_FILE"
        echo "✅ Bot stopped successfully."
        exit 0
    fi
fi

# Fallback kill
pkill -f "full_auto_booker.py"
rm -f "$PID_FILE"
echo "✅ Bot stopped."
