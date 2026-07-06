import subprocess

import redis
import requests

results = {}


def check(name, fn):
    try:
        fn()
        results[name] = "PASS"
        print(f"  [PASS] {name}")
    except Exception as e:
        results[name] = f"FAIL: {e}"
        print(f"  [FAIL] {name}: {e}")


def run_api_inside_container(path: str):
    result = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "api-gateway",
            "python",
            "-c",
            (
                "import sys, urllib.request; "
                f"resp = urllib.request.urlopen('http://127.0.0.1:8000{path}'); "
                "print(resp.status); "
                "sys.exit(0 if 200 <= resp.status < 400 else 1)"
            ),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr or result.stdout


print("\n=== RELIABILITY ===")
check("Health check endpoint", lambda: run_api_inside_container("/health"))
check("API Gateway responds", lambda: run_api_inside_container("/docs"))

print("\n=== OBSERVABILITY ===")
check("Prometheus up", lambda: requests.get("http://localhost:9090/-/healthy").raise_for_status())
check("Grafana up", lambda: requests.get("http://localhost:3000/api/health").raise_for_status())
check("Metrics endpoint exposed", lambda: run_api_inside_container("/metrics"))

print("\n=== SECURITY ===")


def check_unauthorized():
    result = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "api-gateway",
            "python",
            "-c",
            (
                "import http.client, sys; "
                "conn = http.client.HTTPConnection('127.0.0.1', 8000); "
                "conn.request('GET', '/admin'); "
                "resp = conn.getresponse(); "
                "status = resp.status; "
                "print(status); "
                "conn.close(); "
                "sys.exit(0 if status in (401, 403, 404) else 1)"
            ),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr or result.stdout


check("Unauthorized request rejected", check_unauthorized)

print("\n=== VECTOR STORE ===")
check("Qdrant healthy", lambda: requests.get("http://localhost:6333/healthz").raise_for_status())


def check_collection_exists():
    r = requests.get("http://localhost:6333/collections/documents")
    r.raise_for_status()


check("Collection exists", check_collection_exists)

print("\n=== FEATURE STORE ===")
check("Redis reachable", lambda: redis.Redis(host="localhost", port=6379).ping())

print("\n=== KAFKA ===")


def check_kafka_topics():
    result = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "kafka-topics",
            "--list",
            "--bootstrap-server",
            "kafka:29092",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "data.raw" in result.stdout


check("Kafka topics exist", check_kafka_topics)

passed = sum(1 for v in results.values() if v == "PASS")
total = len(results)
score = (passed / total) * 100
print(f"\n{'=' * 40}")
print(f"Production Readiness Score: {passed}/{total} = {score:.0f}%")
print(f"Target: >80% - Status: {'READY' if score >= 80 else 'NOT READY'}")
