#!/bin/sh

set -e

echo "Starting Tensorflow Serving..."
tensorflow_model_server \
    --port=8500 \
    --rest_api_port=8501    \
    --model_name=tip_model  \
    --model_base_path=/models/tip_model &

SERVER_PID=$!

echo "Waiting for model to become available..."
/app/.venv/bin/python3 -c "
import time
import requests

for _ in range(60):
    try:
        r = requests.get('http://localhost:8501/v1/models/tip_model')
        if 'AVAILABLE' in r.text:
            print('Model is ready.')
            break
    except requests.exceptions.ConnectionError:
        pass
    time.sleep(1)
else:
    raise SystemExit('Model did not become ready in time')
"

/app/.venv/bin/python3 /app/src/warehouse/predict.py "$@"

kill $SERVER_PID
