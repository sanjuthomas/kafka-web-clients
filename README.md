# Kafka Web Clients

A reactive Spring Boot application for producing and consuming Kafka messages from the browser. Stream records live over WebSocket and send test messages without leaving the page.

## Stack

- Spring WebFlux (reactive)
- Reactor Kafka (`reactor-kafka`)
- WebSocket for browser streaming
- REST API for config validation and message production

## Prerequisites

### Install Maven (macOS)

Install Maven globally with Homebrew:

```bash
brew install maven
```

Verify the install:

```bash
mvn -version
```

You should see Maven and Java version details in the output.

### Maven Wrapper (`mvnw`)

`mvnw` is not installed globally. It is a project-local script that pins a Maven version for the repo.

If this project does not yet include `mvnw`, generate it from the project root (requires Maven installed first):

```bash
cd kafka-web-clients
mvn wrapper:wrapper
chmod +x mvnw
```

That creates `mvnw`, `mvnw.cmd`, and files under `.mvn/wrapper/`. Commit those files so others can build without installing Maven.

After that, use `./mvnw` instead of `mvn` in the commands below.

## Run

With the Maven Wrapper:

```bash
./mvnw spring-boot:run
```

Or with a globally installed Maven:

```bash
mvn spring-boot:run
```

Open [http://localhost:8080](http://localhost:8080).

## Usage

1. Enter **Kafka bootstrap servers** (e.g. `localhost:9092`).
2. Enter the **topic name** to use.
3. Optionally add extra client properties (`key=value`, one per line) for SASL/SSL or other cluster settings.
4. Click **Submit Config** — the app verifies broker connectivity and that the topic exists, then locks the fields. Use **Edit Config** to change them (only while not streaming).
5. Click **Start Streaming** — opens a WebSocket, connects a reactive Kafka consumer, and pushes each record to the page as it arrives.
6. Use the **Producer** panel to send test messages to the same topic while you watch the stream.
7. Click **Stop Streaming** — stops the consumer and closes the WebSocket.

## REST API

### Validate config

`POST /api/config/validate`

```json
{ "bootstrapServers": "localhost:9092", "topic": "my-topic", "additionalProperties": "" }
```

Returns `{ "valid": true, "message": "..." }` or `{ "valid": false, "error": "..." }`.

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

Returns `{ "success": true, "message": "...", "partition": 0, "offset": 42 }` or `{ "success": false, "error": "..." }`.

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
