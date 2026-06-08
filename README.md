# Kafka Browser Consumer

A reactive Spring Boot application that consumes messages from a single Kafka topic and streams them to the browser in real time over WebSocket.

## Stack

- Spring WebFlux (reactive)
- Reactor Kafka (`reactor-kafka`)
- WebSocket for browser streaming

## Run

```bash
./mvnw spring-boot:run
```

Open [http://localhost:8080](http://localhost:8080).

## Usage

1. Enter **Kafka bootstrap servers** (e.g. `localhost:9092`).
2. Enter the **topic name** to consume.
3. Optionally add extra consumer properties (`key=value`, one per line) for SASL/SSL or other cluster settings.
4. Click **Submit Config** — fields become read-only. Use **Edit Config** to change them (only while not streaming).
5. Click **Start Streaming** — opens a WebSocket, connects a reactive Kafka consumer, and pushes each record to the page as it arrives.
6. Click **Stop Streaming** — stops the consumer and closes the WebSocket.

## WebSocket protocol

Client → server:

```json
{ "action": "start", "config": { "bootstrapServers": "...", "topic": "...", "additionalProperties": "" } }
{ "action": "stop" }
```

Server → client:

```json
{ "type": "status", "payload": "Streaming started" }
{ "type": "record", "key": "...", "payload": "...", "partition": 0, "offset": 42, "timestamp": 1710000000000 }
{ "type": "error", "error": "..." }
```
