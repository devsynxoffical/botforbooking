#!/usr/bin/env bash
set -e

PORT="${PORT:-8000}"

echo "=================================================================="
echo " 🚂 Starting Cita Previa Monitor on Railway"
echo "  - Port: $PORT"
echo "  - Python: $(python --version)"
echo "=================================================================="

# Start live ultra-fast monitor in the background
echo "⚡ Launching Live Monitor Daemon..."
python fast_monitor.py --interval 3.0 &

# Start Web Queue & Admin API in foreground
echo "🌐 Launching Queue & Admin Dashboard on port $PORT..."
exec uvicorn queue_server:app --host 0.0.0.0 --port "$PORT"

