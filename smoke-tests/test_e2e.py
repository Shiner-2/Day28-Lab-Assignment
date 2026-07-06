import json
import subprocess
import time

import requests

BASE_URL = "http://localhost:8000"


class LocalDockerResponse:
    def __init__(self, status_code: int, payload: str):
        self.status_code = status_code
        self._payload = payload

    @property
    def text(self):
        return self._payload

    def json(self):
        return json.loads(self._payload)


def docker_api_request(method: str, path: str, payload: dict | None = None) -> LocalDockerResponse:
    body_literal = "None" if payload is None else repr(json.dumps(payload))
    python_code = (
        "import http.client, json;"
        "conn = http.client.HTTPConnection('127.0.0.1', 8000);"
        f"body={body_literal};"
        f"method='{method}';"
        "data=None if body is None else body.encode();"
        f"conn.request(method, '{path}', body=data, headers={{'Content-Type': 'application/json'}});"
        "resp=conn.getresponse();"
        "print(json.dumps({'status': resp.status, 'body': resp.read().decode()}));"
        "conn.close();"
    )
    result = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "api-gateway",
            "python",
            "-c",
            python_code,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    parsed = json.loads(result.stdout.strip())
    return LocalDockerResponse(parsed["status"], parsed["body"])


def api_request(method: str, path: str, payload: dict | None = None, timeout: float = 30):
    try:
        response = requests.request(method, f"{BASE_URL}{path}", json=payload, timeout=timeout)
        if response.status_code != 404:
            return response
    except requests.RequestException:
        pass
    return docker_api_request(method, path, payload)


class TestHappyPath:
    def test_full_inference_returns_200(self):
        resp = api_request(
            "POST",
            "/api/v1/chat",
            {"query": "What is platform engineering?", "embedding": [0.1] * 384},
            timeout=30,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "answer" in data
        assert len(data["answer"]) > 10
        assert data["latency_ms"] < 2000

    def test_health_check_passes(self):
        resp = api_request("GET", "/health", timeout=5)
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


class TestDataIngestion:
    def test_kafka_ingest_and_qdrant_store(self):
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        producer.send("data.raw", {"id": "smoke_001", "text": "smoke test document"})
        producer.flush()

        time.sleep(10)

        resp = requests.get("http://localhost:6333/collections/documents")
        assert resp.status_code == 200
        count = resp.json()["result"]["points_count"]
        assert count > 0


class TestObservability:
    def test_prometheus_scrapes_api_gateway(self):
        resp = requests.get(
            "http://localhost:9090/api/v1/query",
            params={"query": "up{job='api-gateway'}"},
        )
        assert resp.status_code == 200
        result = resp.json()["data"]["result"]
        assert len(result) > 0
        assert result[0]["value"][1] == "1"

    def test_grafana_dashboard_accessible(self):
        resp = requests.get("http://localhost:3000/api/health", auth=("admin", "admin"))
        assert resp.status_code == 200


class TestFailurePath:
    def test_invalid_request_returns_422(self):
        resp = api_request("POST", "/api/v1/chat", {})
        assert resp.status_code in [400, 422]

    def test_timeout_handled_gracefully(self):
        try:
            requests.post(
                f"{BASE_URL}/api/v1/chat",
                json={"query": "test", "embedding": [0.1] * 384},
                timeout=0.001,
            )
        except requests.exceptions.Timeout:
            pass

        health = api_request("GET", "/health", timeout=5)
        assert health.status_code == 200


class TestFeatureStore:
    def test_feast_redis_has_features(self):
        import redis

        r = redis.Redis(host="localhost", port=6379, decode_responses=True)
        keys = r.keys("feature:*")
        assert len(keys) > 0, "No features found in Feast store"
