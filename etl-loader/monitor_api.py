#!/usr/bin/env python3
from fastapi import FastAPI
import yaml
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

app = FastAPI()

@app.get("/metrics")
def metrics():
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)

@app.get("/status")
def status():
    return {"status": "ok", "ts": __import__('datetime').datetime.utcnow().isoformat() + "Z"}
