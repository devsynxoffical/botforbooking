#!/usr/bin/env bash
# Runner for Cita Previa Extranjería Assistant
cd "$(dirname "$0")"

if [ ! -d ".venv" ]; then
    echo "Virtual environment not found. Please run the setup first."
    exit 1
fi

echo "Starting Cita Previa Extranjería Assistant..."
.venv/bin/python assistant.py

