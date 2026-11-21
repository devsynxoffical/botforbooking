#!/usr/bin/env bash
# Launch Google Chrome with Remote Debugging port 9222 enabled
# This allows Playwright in auto_booker.py to connect to your existing Chrome browser
# and route all requests through your active Urban VPN extension (Spain IP)!

echo "=========================================================="
echo "🚀 Launching Google Chrome with CDP Debug Port 9222..."
echo "=========================================================="

/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --remote-debugging-port=9222 "https://icp.administracionelectronica.gob.es/icpplus/index.html" &

echo "✅ Chrome is now listening on http://127.0.0.1:9222"
echo "👉 You can now run:"
echo "   python3 auto_booker.py --cdp --order-id ORDER-698139"
echo "   OR"
echo "   python3 auto_booker.py --cdp --auto-pick 'Barcelona' 'POLICIA-TOMA DE HUELLA'"
