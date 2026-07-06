# Submission Answers - Lab 28

## 1. Architecture Trade-offs

My platform balances performance, reliability, and maintainability by separating fast online serving from slower data processing. Kafka decouples producers and consumers, Qdrant serves low-latency retrieval, Redis acts as a lightweight online feature store, and the API Gateway keeps the serving interface simple. This improves performance because retrieval and inference requests are isolated from background ingestion workloads.

For reliability, I used service separation and graceful fallback. The API Gateway can still respond in local fallback mode when the remote LLM endpoint is unavailable. The integration worker continuously rebuilds the expected state in Delta Lake, Redis, and Qdrant, which reduces manual recovery effort. Monitoring through Prometheus and Grafana helps detect failures quickly.

For maintainability, I kept the stack modular. Each component has one clear responsibility: ingestion, orchestration, storage, retrieval, serving, or monitoring. This makes the platform easier to debug, extend, and test. The trade-off is that a multi-service architecture is more complex to operate than a monolith, but it is more realistic for production-style AI platforms.

## 2. Handling Local-Kaggle Disconnection

In the hybrid design, the local platform treats Kaggle-hosted model serving as an external dependency. If the connection to Kaggle is interrupted, the system does not need to stop completely. The API Gateway can switch to local fallback mode and still return deterministic responses for health checks, smoke tests, and local demos.

This fallback is useful because development and validation should not be blocked by tunnel instability, Kaggle session timeout, or missing GPU availability. In a full production setting, I would extend this with retry logic, circuit breaking, cached responses, and possibly a secondary model endpoint. In this lab, the key idea is graceful degradation rather than full outage.

## 3. Kafka and Event-driven Decoupling

Kafka decouples the platform by letting producers publish events without knowing which downstream services will consume them. The ingestion layer sends raw records to the `data.raw` topic, and downstream consumers can independently process those events into Delta Lake, Redis features, and Qdrant vectors.

This design improves flexibility because new consumers can be added later without changing the producer. It also improves fault isolation because a temporary failure in one downstream system does not require rewriting the ingestion source. Event-driven architecture is especially suitable for AI platforms because data preparation, feature generation, vector indexing, and model-serving enrichment often evolve at different speeds.

## 4. Observability Design

Observability is implemented across metrics, logs, and service health checks. The FastAPI API Gateway exposes Prometheus metrics, which Prometheus scrapes and Grafana visualizes. This gives visibility into request activity and platform availability. Health endpoints are used in the production readiness script to validate service reachability.

In addition, logs from Docker services help identify runtime failures such as dependency mismatches or worker startup issues. The platform also includes a LangSmith verification script for tracing support when an API key is configured, although the local-only submission does not require it. Overall, the observability approach combines service health, metrics collection, and operational logs to make debugging and validation easier.

## 5. Failure Handling and Graceful Degradation

If a component such as Qdrant or Kafka crashes, the system degrades based on the failed dependency instead of fully collapsing. For example, if the remote LLM is unavailable, the API Gateway still responds in fallback mode. If Qdrant is unavailable, retrieval quality is affected, but the platform can still expose health information and basic service functionality. If Kafka is temporarily down, producers and consumers are isolated so the failure stays localized to the ingestion path.

The platform also uses restart-friendly container services, seeded local data, and an integration worker that reconstructs expected downstream state. This helps recovery after restart and reduces manual repair work. In a more advanced production system, I would add persistent queues, dead-letter topics, alerting rules, and automated restart backoff, but the current design already demonstrates graceful degradation and recovery-oriented thinking.
