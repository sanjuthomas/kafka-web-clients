# Kafka Web Clients

Connect to a Kafka topic and produce and consume messages from your browser.

## Dataflow

![Kafka Web Clients dataflow](docs/dataflow-mindmap.png)

## Features

- Connect to a Kafka cluster and topic from the web UI
- Produce messages to the topic
- Consume messages from the topic in real time
- Administer the cluster from a separate UI: list user topics, create/delete topics, inspect consumer group offsets, and reset offsets

## Stack

- Spring WebFlux
- Reactor Kafka (`reactor-kafka`)
- WebSocket (`/ws/stream`) for live consumption
- REST API for config validation, message production, and cluster administration

## Requirements

- Java 21+
- Maven 3.9+ (or generate the Maven Wrapper — see below)

## Get started

```bash
mvn spring-boot:run
```

Open [http://localhost:8080](http://localhost:8080) for the **Stream** page, or [http://localhost:8080/admin](http://localhost:8080/admin) for **Admin**.

## Docker

Build and run the app in a container:

```bash
docker build -t kafka-web-clients .
docker run --rm -p 8080:8080 kafka-web-clients
```

Or use Compose:

```bash
docker compose up --build
```

Open [http://localhost:8080](http://localhost:8080) for the **Stream** page, or [http://localhost:8080/admin](http://localhost:8080/admin) for **Admin**.

When the app runs in Docker and Kafka runs on your host machine, use `host.docker.internal:9092` as the bootstrap servers in the UI (macOS/Windows). On Linux, use your host IP or run Kafka in the same Docker network.

### Docker Hub

CI publishes a multi-arch image (`linux/amd64`, `linux/arm64`) to Docker Hub on every push to `main` and on version tags (`v*`, e.g. `v1.0.0`).

Pull the latest image:

```bash
docker pull sanjuthomas/kafka-web-clients:latest
docker run --rm -p 8080:8080 sanjuthomas/kafka-web-clients:latest
```

Tagged releases are also published as semver tags (e.g. `1.0.0`) and as the short Git commit SHA.

### Maven Wrapper (`mvnw`)

`mvnw` is not installed globally. It is a project-local script that pins a Maven version for the repo.

If this project does not yet include `mvnw`, generate it from the project root:

```bash
mvn wrapper:wrapper
chmod +x mvnw
./mvnw spring-boot:run
```

### Install Maven (macOS)

```bash
brew install maven
mvn -version
```

## Usage

The app has two pages, linked from the top navigation:

| Page | URL | Purpose |
|------|-----|---------|
| **Stream** | [http://localhost:8080/](http://localhost:8080/) | Produce and consume messages for a topic |
| **Admin** | [http://localhost:8080/admin](http://localhost:8080/admin) | Cluster administration |

### Stream page

The Stream page has three panes: **Configuration**, **Producer**, and **Live Stream**.

1. In **Configuration**, enter **Kafka bootstrap servers** (e.g. `localhost:9092`), the **topic name**, and optional client properties (`key=value`, one per line) for SASL/SSL or other settings.
2. Click **Submit Config**. The app validates broker connectivity and that the topic exists. On success, the config form collapses and the Producer pane appears.
3. In **Live Stream**, click **Start Streaming** to open a WebSocket consumer and display incoming records.
4. In **Producer**, enter an optional message key and payload, then click **Send Message** to publish test records to the same topic.
5. Click **Stop Streaming** in the Live Stream pane to disconnect the consumer.

Use **Edit Config** (Configuration pane) to change settings when not streaming.

### Admin page

The Admin page has **Configuration** (bootstrap servers only) and an **Admin** pane that unlocks after you submit config.

1. Enter **Kafka bootstrap servers** and click **Submit Config**. The app verifies broker connectivity.
2. Use the admin tools:
   - **List User Topics** — shows user-defined topics (internal topics such as `__consumer_offsets` are excluded)
   - **Create Topic** — create one topic at a time with partition count and replication factor
   - **Delete Topic** — delete one topic at a time (confirmation required)
   - **Consumer Groups** — for a given topic, list groups with committed offsets per partition
   - **Reset Offset** — reset a consumer group's committed offsets on a topic to a specific value (e.g. `0`)

Admin operations require broker ACLs for topic create/delete, group describe, and offset alter where applicable.

## REST API

### Validate config

`POST /api/config/validate`

```json
{
  "bootstrapServers": "localhost:9092",
  "topic": "my-topic",
  "additionalProperties": ""
}
```

Response:

```json
{ "valid": true, "message": "Connected to Kafka. Topic 'my-topic' exists with 1 partition(s)." }
```

```json
{ "valid": false, "error": "Could not connect to Kafka at localhost:9092. Check that the broker is running and reachable." }
```

### Produce message

`POST /api/produce`

```json
{
  "bootstrapServers": "localhost:9092",
  "topic": "my-topic",
  "additionalProperties": "",
  "key": "optional-key",
  "payload": "hello from browser"
}
```

Response:

```json
{
  "success": true,
  "message": "Message sent to partition 0 at offset 42.",
  "partition": 0,
  "offset": 42
}
```

```json
{ "success": false, "error": "Message payload is required" }
```

### Validate cluster (admin)

`POST /api/config/validate-cluster`

```json
{
  "bootstrapServers": "localhost:9092",
  "additionalProperties": ""
}
```

Response:

```json
{ "valid": true, "message": "Connected to Kafka cluster (cluster-id) at localhost:9092." }
```

### List user topics

`POST /api/admin/topics/list`

```json
{
  "bootstrapServers": "localhost:9092",
  "additionalProperties": ""
}
```

Response:

```json
{ "success": true, "topics": ["events", "orders"], "message": "Found 2 user topic(s)." }
```

### Create topic

`POST /api/admin/topics/create`

```json
{
  "bootstrapServers": "localhost:9092",
  "additionalProperties": "",
  "topic": "new-topic",
  "partitions": 1,
  "replicationFactor": 1
}
```

### Delete topic

`POST /api/admin/topics/delete`

```json
{
  "bootstrapServers": "localhost:9092",
  "additionalProperties": "",
  "topic": "old-topic"
}
```

### List consumer groups for a topic

`POST /api/admin/consumer-groups`

```json
{
  "bootstrapServers": "localhost:9092",
  "additionalProperties": "",
  "topic": "my-topic"
}
```

Response:

```json
{
  "success": true,
  "groups": [
    { "groupId": "my-group", "partitions": [{ "partition": 0, "offset": 42 }] }
  ],
  "message": "Found 1 consumer group(s) with offsets for this topic."
}
```

### Reset consumer group offset

`POST /api/admin/consumer-groups/reset-offset`

```json
{
  "bootstrapServers": "localhost:9092",
  "additionalProperties": "",
  "topic": "my-topic",
  "consumerGroup": "my-group",
  "offset": 0
}
```

Response:

```json
{
  "success": true,
  "message": "Reset committed offsets for consumer group 'my-group' on topic 'my-topic' to 0."
}
```

## WebSocket protocol

Endpoint: `ws://localhost:8080/ws/stream`

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

## Project layout

```
src/main/java/com/sanjuthomas/kafkawebclients/
  controller/   REST endpoints (config validation, produce, admin)
  handler/      WebSocket streaming handler
  service/      Kafka consumer, producer, connectivity, and admin services
  support/      Shared Kafka client property builder and AdminClient facade
  model/        Request/response and WebSocket message types
src/main/resources/static/
  index.html    Stream page (produce + live consume)
  admin.html    Admin page (cluster administration)
  css/app.css   Shared styles
```

## License

MIT — see [LICENSE](LICENSE).
