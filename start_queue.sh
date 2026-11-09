#!/usr/bin/env bash
# Start the Order Intake & Queue Server
cd "$(dirname "$0")"

if [ ! -d ".venv" ]; then
    echo "Virtual environment not found."
    exit 1
fi

echo "Starting Appointment Order Intake & Queue Server on http://127.0.0.1:8000"
echo "  - Customer Intake & Lookup: http://127.0.0.1:8000"
echo "  - Admin Queue Dashboard:   http://127.0.0.1:8000/admin"
echo "  - Interactive API Docs:    http://127.0.0.1:8000/docs"
echo ""
.venv/bin/uvicorn queue_server:app --host 127.0.0.1 --port 8000 --reload

