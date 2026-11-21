#!/usr/bin/env bash
# Start 24/7 Full Autonomous Cita Previa Booker in Background

LOG_FILE="full_auto.log"
PID_FILE="full_auto.pid"

if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if ps -p "$OLD_PID" > /dev/null 2>&1; then
        echo "⚠️ Full Auto Booker is already running (PID: $OLD_PID)."
        echo "To view live logs: tail -f $LOG_FILE"
        echo "To stop: ./stop_full_auto.sh"
        exit 0
    fi
fi

echo "=========================================================="
echo "🚀 Starting Full Autonomous Cita Previa Auto-Booker..."
echo "=========================================================="
nohup python3 -u full_auto_booker.py > "$LOG_FILE" 2>&1 &
NEW_PID=$!
echo $NEW_PID > "$PID_FILE"

echo "✅ Bot is now running in background (PID: $NEW_PID)!"
echo "📄 Live log output: $LOG_FILE"
echo "👉 Run 'tail -f full_auto.log' to watch live checks."
echo "👉 Run './stop_full_auto.sh' to stop."
