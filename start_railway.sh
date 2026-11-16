#!/bin/sh

PORT="${PORT:-8000}"

echo "=================================================================="
echo " 🚂 Starting Cita Previa Booking & Monitor System on Railway"
echo "  - Port: $PORT"
echo "=================================================================="

# Ensure permissions on /tmp/tor_data if running as debian-tor or root
mkdir -p /tmp/tor_data 2>/dev/null
chmod 700 /tmp/tor_data 2>/dev/null

# Launch Tor in background using debian-tor if available
if command -v tor >/dev/null 2>&1; then
    echo "🧅 Starting Built-in Tor Daemon (Spain Exit Nodes)..."
    if id debian-tor >/dev/null 2>&1; then
        chown -R debian-tor:debian-tor /tmp/tor_data 2>/dev/null
        chmod 700 /tmp/tor_data 2>/dev/null
        su -s /bin/sh debian-tor -c 'tor --SOCKSPort 127.0.0.1:9050 --HTTPTunnelPort 127.0.0.1:9080 --ControlPort 127.0.0.1:9051 --ExitNodes "{es}" --StrictNodes 0 --CookieAuthentication 0 --DataDirectory /tmp/tor_data' >/tmp/tor.log 2>&1 &
    else
        tor --SOCKSPort 127.0.0.1:9050 --HTTPTunnelPort 127.0.0.1:9080 --ControlPort 127.0.0.1:9051 --ExitNodes "{es}" --StrictNodes 0 --CookieAuthentication 0 --DataDirectory /tmp/tor_data >/tmp/tor.log 2>&1 &
    fi
else
    echo "⚠️ Tor executable not found, skipping Tor launch."
fi

# Set default proxy environment variables to local Tor SOCKS5
export PROXY_URL="${PROXY_URL:-socks5://127.0.0.1:9050}"
export HTTPS_PROXY="${HTTPS_PROXY:-socks5://127.0.0.1:9050}"
export HTTP_PROXY="${HTTP_PROXY:-socks5://127.0.0.1:9050}"

# Start Web Queue & Admin API in foreground so Railway binds immediately
echo "🌐 Launching Application Web Server on port $PORT..."
exec python3 -m uvicorn queue_server:app --host 0.0.0.0 --port "$PORT"


