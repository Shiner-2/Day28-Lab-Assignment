import requests


def check_prometheus():
    resp = requests.get(
        "http://localhost:9090/api/v1/query",
        params={"query": 'http_requests_total{job="api-gateway"}'},
    )
    data = resp.json()
    assert data["status"] == "success"
    print("Integration 9 OK: Prometheus metrics flowing")


def check_langsmith():
    import os

    from langsmith import Client

    api_key = os.environ.get("LANGCHAIN_API_KEY")
    if not api_key:
        raise RuntimeError("LANGCHAIN_API_KEY is not configured")

    client = Client(api_key=api_key)
    runs = list(client.list_runs(project_name="lab28-platform", limit=1))
    assert len(runs) > 0
    print("Integration 10 OK: LangSmith traces visible")


if __name__ == "__main__":
    check_prometheus()
    check_langsmith()
