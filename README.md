# Kafka Browser Consumer

A reactive Spring Boot application that consumes messages from a single Kafka topic and streams them to the browser in real time over WebSocket.

## Stack

- Spring WebFlux (reactive)
- Reactor Kafka (`reactor-kafka`)
- WebSocket for browser streaming

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
cd kafka-browser-consumer
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
