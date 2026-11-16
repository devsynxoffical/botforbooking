#!/usr/bin/env bash

PORT="${PORT:-8000}"

echo "=================================================================="
echo " 🚂 Starting Cita Previa Monitor on Railway"
echo "  - Port: $PORT"
echo "  - Python: $(python --version 2>&1)"
echo "=================================================================="

# Ensure Tor directories exist and have proper permissions
mkdir -p /var/lib/tor /var/log/tor /etc/tor
chown -R debian-tor:debian-tor /var/lib/tor /var/log/tor /etc/tor 2>/dev/null || true
chmod 700 /var/lib/tor 2>/dev/null || true

# 1. Start Tor in background
if command -v tor >/dev/null 2>&1; then
    echo "🧅 Starting Built-in Tor Daemon (Spain Exit Nodes)..."
    tor --runasdaemon 1 --DataDirectory /var/lib/tor -f /etc/tor/torrc 2>/dev/null || \
    (tor -f /etc/tor/torrc >/dev/null 2>&1 &) || true
    sleep 2
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

