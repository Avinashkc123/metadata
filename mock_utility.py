"""Helpers for email ingest, event polling, and Kafka validation."""

from __future__ import annotations

import os
import time

import requests

_API_BASE = os.environ.get("METADATA_API_BASE", "http://127.0.0.1:8010").rstrip("/")
INGEST_URL = f"{_API_BASE}/ingest"
EVENTS_URL = f"{_API_BASE}/events"


def _response_has_event(data: object) -> bool:
    if not isinstance(data, dict):
        return False
    if data.get("found") is True:
        return True
    events = data.get("events")
    if isinstance(events, list) and len(events) > 0:
        return True
    if data.get("event") is not None:
        return True
    return False


def send_email_api(file_name: str) -> str:
    print("[API] POST /ingest")
    print(f"[API REQUEST] fileName={file_name}")
    try:
        resp = requests.post(
            INGEST_URL,
            json={"fileName": file_name},
            timeout=30,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        raise Exception(f"Ingest request failed: {e}") from e
    try:
        body = resp.json()
    except ValueError as e:
        raise Exception(f"Ingest invalid JSON response: {e}") from e
    submission_id = body.get("submissionId")
    if submission_id is None:
        raise Exception("Ingest response missing submissionId")
    submission_id_str = str(submission_id).strip()
    if not submission_id_str:
        raise Exception("Ingest response missing submissionId")
    print(f"[API RESPONSE] submissionId={submission_id_str}")
    return submission_id_str


def wait_for_event(submission_id: str, stage: str, timeout: float = 15) -> bool:
    start = time.monotonic()
    deadline = start + timeout
    while True:
        if time.monotonic() >= deadline:
            print("[POLL TIMEOUT]")
            raise Exception(
                f"Event poll timed out after {timeout}s "
                f"(submissionId={submission_id!r}, stage={stage!r})"
            )
        print(f"[POLL] checking submissionId={submission_id} stage={stage}")
        try:
            resp = requests.get(
                EVENTS_URL,
                params={"submissionId": submission_id, "stage": stage},
                timeout=30,
            )
            if resp.status_code == 200:
                try:
                    data = resp.json()
                except ValueError:
                    data = None
                if _response_has_event(data):
                    print("[POLL SUCCESS]")
                    return True
        except requests.RequestException:
            pass

        if time.monotonic() >= deadline:
            print("[POLL TIMEOUT]")
            raise Exception(
                f"Event poll timed out after {timeout}s "
                f"(submissionId={submission_id!r}, stage={stage!r})"
            )
        print("[POLL] retrying...")
        time.sleep(1)


def validate_kafka_event(submission_id: str, stage: str) -> bool:
    print("[VALIDATE] checking Kafka event")
    wait_for_event(submission_id, stage)
    return True
