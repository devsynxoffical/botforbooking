#!/bin/sh

PORT="${PORT:-8000}"

echo "=================================================================="
echo " 🚂 Starting Cita Previa Monitor on Railway"
echo "  - Port: $PORT"
echo "  - Python: $(python3 --version 2>&1 || python --version 2>&1)"
echo "=================================================================="

# 1. Start Tor in background (isolated in /tmp/tor_data, no setuid permissions needed)
if command -v tor >/dev/null 2>&1; then
    echo "🧅 Starting Built-in Tor Daemon (Spain Exit Nodes)..."
    mkdir -p /tmp/tor_data
    chmod 700 /tmp/tor_data
    tor --SOCKSPort 127.0.0.1:9050 --ControlPort 127.0.0.1:9051 --ExitNodes "{es}" --StrictNodes 0 --CookieAuthentication 0 --DataDirectory /tmp/tor_data >/tmp/tor.log 2>&1 &
    sleep 3
else
    echo "⚠️ Tor executable not found, skipping Tor launch."
fi

# 2. Set default proxy to Tor if not overridden
if [ -z "$PROXY_URL" ] && [ -z "$HTTPS_PROXY" ]; then
    echo "⚡ Activating built-in Tor SOCKS5 proxy: socks5://127.0.0.1:9050"
    export PROXY_URL="socks5://127.0.0.1:9050"
    export HTTPS_PROXY="socks5://127.0.0.1:9050"
    export HTTP_PROXY="socks5://127.0.0.1:9050"
fi

# 3. Start live ultra-fast monitor in the background
echo "⚡ Launching Live Monitor Daemon..."
python fast_monitor.py --interval 3.0 &

# 4. Start Web Queue & Admin API in foreground
echo "🌐 Launching Queue & Admin Dashboard on port $PORT..."
exec uvicorn queue_server:app --host 0.0.0.0 --port "$PORT"

