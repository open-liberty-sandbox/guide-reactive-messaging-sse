# feature/my-solution — Change Log

This document summarises the key additions that implement a reactive SSE pipeline: a System Microservice publishes CPU load metrics to Kafka, a Backend For Frontend (BFF) Microservice consumes those messages and streams them to browser clients via Server-Sent Events, and a Frontend serves the UI that renders the live data.

---

## 1. Publish system load metrics to Kafka (`SystemService.java`)

**Commit:** `feat: emit SystemLoad events on outgoing systemLoad channel`

### What changed

The `sendSystemLoad()` method was added to `SystemService` with the `@Outgoing("systemLoad")` annotation, returning a reactive `Publisher<SystemLoad>`. The emission interval is driven by an injectable `UPDATE_INTERVAL` config property (default 5 seconds):

```java
@Inject
@ConfigProperty(name = "UPDATE_INTERVAL", defaultValue = "5")
private long updateInterval;

@Outgoing("systemLoad")
public Publisher<SystemLoad> sendSystemLoad() {
    return Flowable.interval(updateInterval, TimeUnit.SECONDS)
                   .map((interval -> new SystemLoad(getHostname(),
                         Double.valueOf(OS_MEAN.getSystemLoadAverage()))));
}
```

A helper `getHostname()` resolves the container hostname via `InetAddress.getLocalHost()`, falling back to the `HOSTNAME` environment variable.

### Why

`@Outgoing("systemLoad")` declares that this method produces messages onto a named channel. The MicroProfile Reactive Messaging runtime subscribes to the returned `Publisher` and forwards each emitted item to the configured Kafka connector. Using RxJava3's `Flowable.interval` produces a periodic, back-pressure-aware stream — the runtime is never pushed more items than it can handle.

Externalising the interval via `@ConfigProperty` allows different environments (dev, test, prod) to tune the emission frequency without recompilation.

---

## 2. Consume Kafka messages and broadcast via SSE (`BFFResource.java`)

**Commit:** `feat: add BFF SSE endpoint and @Incoming systemLoad consumer`

### What changed

`BFFResource` serves three responsibilities: managing SSE client connections, receiving Kafka messages, and broadcasting events to all connected clients.

#### 2a. SSE subscription endpoint

```java
@GET
@Path("/")
@Produces(MediaType.SERVER_SENT_EVENTS)
public void subscribeToSystem(
    @Context SseEventSink sink,
    @Context Sse sse) {

    if (this.sse == null || this.broadcaster == null) {
        this.sse = sse;
        this.broadcaster = sse.newBroadcaster();
    }
    this.broadcaster.register(sink);
    logger.info("New sink registered to broadcaster.");
}
```

A `GET /bff/sse/` request opens a long-lived SSE connection. Each browser tab that connects receives its own `SseEventSink`, which is registered with the shared `SseBroadcaster`. The broadcaster keeps track of all active sinks so that every subsequent `broadcast()` call delivers the event to all connected clients simultaneously.

#### 2b. Kafka consumer bridged to SSE

```java
@Incoming("systemLoad")
public void getSystemLoadMessage(SystemLoad sl) {
    logger.info("Message received from system.load topic. " + sl.toString());
    broadcastData("systemLoad", sl);
}
```

`@Incoming("systemLoad")` tells the MicroProfile Reactive Messaging runtime to call this method for every deserialized `SystemLoad` message arriving from Kafka. No polling or offset management is needed in application code.

#### 2c. Event builder and broadcast helper

```java
private void broadcastData(String name, Object data) {
    if (broadcaster != null) {
        OutboundSseEvent event = sse.newEventBuilder()
                                    .name(name)
                                    .data(data.getClass(), data)
                                    .mediaType(MediaType.APPLICATION_JSON_TYPE)
                                    .build();
        broadcaster.broadcast(event);
    } else {
        logger.info("Unable to send SSE. Broadcaster context is not set up.");
    }
}
```

`sse.newEventBuilder()` constructs an `OutboundSseEvent` with a named event type (`"systemLoad"`), a typed data payload serialized as JSON, and then `broadcaster.broadcast(event)` pushes it to every registered sink in one call.

### Why

The BFF layer decouples the internal Kafka transport from the browser-facing SSE protocol. The browser never knows about Kafka — it only sees a standard SSE stream. `SseBroadcaster` handles fan-out to multiple browser clients transparently, and the null-guard on `broadcaster` prevents a `NullPointerException` if a Kafka message arrives before any client has connected.

---

## 3. Wire channels to Kafka via MicroProfile Config

**Commit:** `feat: add microprofile-config.properties for system and bff Kafka bindings`

### What changed — System Microservice

`system/src/main/resources/META-INF/microprofile-config.properties` binds the outgoing `systemLoad` channel to the `system.load` Kafka topic:

```properties
mp.messaging.connector.liberty-kafka.bootstrap.servers=kafka:9092

mp.messaging.outgoing.systemLoad.connector=liberty-kafka
mp.messaging.outgoing.systemLoad.topic=system.load
mp.messaging.outgoing.systemLoad.key.serializer=org.apache.kafka.common.serialization.StringSerializer
mp.messaging.outgoing.systemLoad.value.serializer=io.openliberty.guides.models.SystemLoad$SystemLoadSerializer
```

### What changed — BFF Microservice

`bff/src/main/resources/META-INF/microprofile-config.properties` binds the incoming `systemLoad` channel to the same `system.load` topic with the JSONB deserializer and a dedicated consumer group:

```properties
mp.messaging.connector.liberty-kafka.bootstrap.servers=kafka:9092

mp.messaging.incoming.systemLoad.connector=liberty-kafka
mp.messaging.incoming.systemLoad.topic=system.load
mp.messaging.incoming.systemLoad.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
mp.messaging.incoming.systemLoad.value.deserializer=io.openliberty.guides.models.SystemLoad$SystemLoadDeserializer
mp.messaging.incoming.systemLoad.group.id=bff
```

### Why

The channel names in `@Outgoing`/`@Incoming` are logical names — they are mapped to a physical Kafka topic via MicroProfile Config. The `group.id=bff` on the BFF consumer ensures that if multiple BFF instances are running, Kafka distributes messages across them rather than delivering every message to every instance. The consumer group is distinct from the test's consumer group (`system-load-status`), so both can read from the same topic independently during integration testing.

---

## 4. Add custom Kafka serializer and deserializer (`SystemLoad.java`)

**Commit:** `feat: add JSONB-backed Kafka serializer and deserializer to SystemLoad model`

### What changed

`SystemLoad` carries two static inner classes implementing Kafka's `Serializer` and `Deserializer` interfaces, backed by Jakarta JSON Binding (JSONB):

```java
public static class SystemLoadSerializer implements Serializer<Object> {
    @Override
    public byte[] serialize(String topic, Object data) {
        return JSONB.toJson(data).getBytes();
    }
}

public static class SystemLoadDeserializer implements Deserializer<SystemLoad> {
    @Override
    public SystemLoad deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        return JSONB.fromJson(new String(data), SystemLoad.class);
    }
}
```

A shared static `Jsonb` instance is used for both:

```java
private static final Jsonb JSONB = JsonbBuilder.create();
```

### Why

Kafka transmits raw bytes — the application is responsible for converting objects to and from bytes. Placing both classes inside `SystemLoad` keeps serialization logic co-located with the model it owns. Both services reference the same inner class paths in their config files, guaranteeing wire format consistency between the System producer and BFF consumer.

---

## 5. Subscribe to SSE events in the browser (`index.js`)

**Commit:** `feat: add frontend EventSource subscription and systemLoad event handler`

### What changed

`index.js` opens an SSE connection to the BFF on page load and registers a named event listener:

```javascript
function initSSE() {
  var source = new EventSource("http://localhost:9084/bff/sse", {
    withCredentials: true,
  });
  source.addEventListener("systemLoad", systemLoadHandler);
}
```

The `systemLoadHandler` parses the JSON event data and updates the DOM table — adding a new row if the hostname is new, or updating the load average cell if it already exists:

```javascript
function systemLoadHandler(event) {
  var system = JSON.parse(event.data);
  if (document.getElementById(system.hostname)) {
    document.getElementById(system.hostname).cells[1].innerHTML =
      system.loadAverage.toFixed(2);
  } else {
    var tableRow = document.createElement("tr");
    tableRow.id = system.hostname;
    tableRow.innerHTML =
      "<td>" +
      system.hostname +
      "</td><td>" +
      system.loadAverage.toFixed(2) +
      "</td>";
    document.getElementById("sysPropertiesTableBody").appendChild(tableRow);
  }
}
```

### Why

`EventSource` is the standard browser API for SSE — it maintains a persistent HTTP connection and fires events by name as the server pushes them. Using a named event type (`"systemLoad"`) lets the same SSE stream carry multiple distinct event types in future without changing the listener wiring. The `{ withCredentials: true }` flag is required because the BFF's CORS policy sets `allowCredentials="true"` on the `/bff/sse` path. Updates are idempotent: the hostname doubles as the DOM element ID so repeated updates overwrite the existing row rather than appending duplicates.

---

## 6. Integration test — verify System Microservice publishes to Kafka (`SystemServiceIT.java`)

**Commit:** `feat: create SystemServiceIT integration test`

### What changed

`SystemServiceIT.java` verifies that the System Microservice publishes valid `SystemLoad` messages to the `system.load` Kafka topic. It uses Testcontainers to spin up real Kafka and System containers:

```java
private static KafkaContainer kafkaContainer =
    new KafkaContainer("apache/kafka:latest")
        .withListener("kafka:19092")
        .withNetwork(network);

private static GenericContainer<?> systemContainer =
    new GenericContainer<>(systemImage)
        .withNetwork(network)
        .withExposedPorts(9083)
        .waitingFor(Wait.forHttp("/health/ready").forPort(9083))
        .dependsOn(kafkaContainer);
```

A `KafkaConsumer` subscribes directly to `system.load` with `AUTO_OFFSET_RESET_CONFIG=earliest` and polls for up to 60 seconds:

```java
@Test
public void testCpuStatus() {
    ConsumerRecords<String, SystemLoad> records =
        consumer.poll(Duration.ofMillis(60 * 1000));
    System.out.println("Polled " + records.count() + " records from Kafka:");

    for (ConsumerRecord<String, SystemLoad> record : records) {
        SystemLoad sl = record.value();
        assertNotNull(sl.hostname);
        assertNotNull(sl.loadAverage);
    }
    consumer.commitAsync();
}
```

The test also supports running against a live `liberty:devc` instance: if port 9083 is already open on `localhost`, containers are skipped and the Kafka bootstrap servers address is set to `localhost:9094` instead.

### Why

The test verifies the System Microservice's core responsibility — that it actually emits `SystemLoad` messages with non-null `hostname` and `loadAverage` fields. By using a real `KafkaContainer` and `SystemLoadDeserializer`, the test exercises the full serialization path end-to-end. The 60-second poll window gives the System Microservice enough time to start up and emit at least one message at its default 5-second interval. Testing at the Kafka boundary means the BFF and frontend are not required to be running — the System Microservice is validated in isolation.
