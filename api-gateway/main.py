import os
import time
from typing import Any

import httpx
from fastapi import FastAPI
from pydantic import BaseModel, Field
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI(title="AI Platform API Gateway")
Instrumentator().instrument(app).expose(app)

VLLM_URL = os.environ.get("VLLM_URL", "").rstrip("/")
QDRANT_URL = os.environ.get("QDRANT_URL", "http://qdrant:6333").rstrip("/")
MODEL_NAME = "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4"
EMBEDDING_SIZE = 384


class ChatRequest(BaseModel):
    query: str = Field(min_length=1)
    embedding: list[float] = Field(default_factory=list)


def build_fallback_answer(query: str, context: list[dict[str, Any]]) -> str:
    if context:
        snippets = []
        for item in context[:3]:
            payload = item.get("payload", {})
            text = payload.get("text") or payload.get("summary") or "context available"
            snippets.append(str(text))
        return f"Local fallback answer for '{query}'. Retrieved context: {' | '.join(snippets)}"

    return (
        f"Local fallback answer for '{query}'. External LLM is not configured, "
        "so the gateway is serving a deterministic response."
    )


async def search_context(embedding: list[float]) -> list[dict[str, Any]]:
    vector = embedding or [0.0] * EMBEDDING_SIZE
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.post(
                f"{QDRANT_URL}/collections/documents/points/search",
                json={"vector": vector, "limit": 3, "with_payload": True},
            )
            response.raise_for_status()
        return response.json().get("result", [])
    except Exception:
        return []


async def call_llm(prompt: str) -> dict[str, Any] | None:
    if not VLLM_URL:
        return None

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(
                f"{VLLM_URL}/v1/chat/completions",
                json={
                    "model": MODEL_NAME,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            response.raise_for_status()
        return response.json()
    except Exception:
        return None


@app.post("/api/v1/chat")
async def chat(body: ChatRequest):
    start = time.time()
    context = await search_context(body.embedding)
    prompt = f"Context: {context}\n\nQuery: {body.query}"
    llm_result = await call_llm(prompt)

    if llm_result and llm_result.get("choices"):
        answer = llm_result["choices"][0]["message"]["content"]
        model = llm_result.get("model", MODEL_NAME)
    else:
        answer = build_fallback_answer(body.query, context)
        model = "local-fallback"

    latency = (time.time() - start) * 1000
    return {"answer": answer, "latency_ms": round(latency, 2), "model": model}


@app.get("/health")
def health():
    return {
        "status": "ok",
        "vllm_configured": bool(VLLM_URL),
        "mode": "remote-llm" if VLLM_URL else "local-fallback",
    }
