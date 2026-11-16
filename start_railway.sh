#!/usr/bin/env bash
set -e

PORT="${PORT:-8000}"

echo "=================================================================="
echo " 🚂 Starting Cita Previa Monitor on Railway"
echo "  - Port: $PORT"
echo "  - Python: $(python --version)"
echo "=================================================================="

# 1. Start Tor service with Spanish Exit Node ({es})
echo "🧅 Starting Built-in Tor Service (Spain Exit Nodes)..."
if command -v tor >/dev/null 2>&1; then
    mkdir -p /var/lib/tor /var/log/tor
    chown -R debian-tor:debian-tor /var/lib/tor /var/log/tor /etc/tor 2>/dev/null || true
    chmod 700 /var/lib/tor 2>/dev/null || true
    service tor start || (su -s /bin/sh debian-tor -c "tor -f /etc/tor/torrc &") || tor --runasdaemon 1 --allow-missing-torrc || true

    # Wait up to 12s for Tor SOCKS5 to open
    for i in $(seq 1 12); do
        if nc -z 127.0.0.1 9050 2>/dev/null; then
            echo "✅ Tor SOCKS5 proxy is active and listening on 127.0.0.1:9050!"
            break
        fi
        sleep 1
    done
else
    echo "⚠️ Tor executable not found, proceeding with direct connectivity."
fi

# 2. Set default proxy to Tor if not explicitly overridden
if [ -z "$PROXY_URL" ] && [ -z "$HTTPS_PROXY" ]; then
    echo "⚡ No external proxy specified. Activating built-in Tor (Spain Exit): socks5://127.0.0.1:9050"
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

