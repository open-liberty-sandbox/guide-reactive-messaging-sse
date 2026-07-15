# Key Learnings — Quiz

Test your understanding of the Reactive Messaging SSE guide.

---

**Q1. What are the three services in this guide and what does each one do?**

<details>
<summary>Answer</summary>

| Service                 | Port | Responsibility                                                                                                   |
| ----------------------- | ---- | ---------------------------------------------------------------------------------------------------------------- |
| **System Microservice** | 9083 | Reads its own hostname and CPU load average every 5 seconds and publishes them to Kafka                          |
| **BFF Microservice**    | 9084 | Consumes `SystemLoad` messages from Kafka and re-broadcasts them to browser clients via Server-Sent Events (SSE) |
| **Frontend**            | 9080 | Serves a static HTML/JS page that subscribes to SSE and renders a live table of hostnames and CPU loads          |

The BFF (Backend For Frontend) is the key new layer — it bridges the Kafka message bus to the browser-native SSE protocol.

</details>

---

**Q2. What is the SSE endpoint in the BFF, and what happens when a browser connects to it?**

<details>
<summary>Answer</summary>

The BFF exposes `GET /bff/sse/` with `@Produces(MediaType.SERVER_SENT_EVENTS)`:

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
}
```

When a browser connects, JAX-RS injects a new `SseEventSink` representing that connection. On first connection, a `SseBroadcaster` is created. Each subsequent connection registers its sink with the same broadcaster. The HTTP connection remains open — the browser waits for the server to push events rather than polling.

</details>

---

**Q3. What is a `SseBroadcaster` and why is it used instead of writing directly to a single `SseEventSink`?**

<details>
<summary>Answer</summary>

`SseBroadcaster` is a JAX-RS utility that manages a collection of active `SseEventSink` connections. Calling `broadcaster.broadcast(event)` delivers the event to every registered sink in one call.

Using `broadcaster` instead of a single `sink` is necessary because multiple browser tabs or clients may connect simultaneously — each gets its own `SseEventSink`. If only one sink were stored, all other connected clients would miss events. The broadcaster abstracts the fan-out and also automatically removes sinks that have been closed (e.g., when a tab is closed).

</details>

---

**Q4. How does the BFF connect a Kafka message to an SSE broadcast?**

<details>
<summary>Answer</summary>

Two methods work together:

```java
@Incoming("systemLoad")
public void getSystemLoadMessage(SystemLoad sl) {
    broadcastData("systemLoad", sl);
}

private void broadcastData(String name, Object data) {
    if (broadcaster != null) {
        OutboundSseEvent event = sse.newEventBuilder()
                                    .name(name)
                                    .data(data.getClass(), data)
                                    .mediaType(MediaType.APPLICATION_JSON_TYPE)
                                    .build();
        broadcaster.broadcast(event);
    }
}
```

`@Incoming("systemLoad")` causes the MicroProfile Reactive Messaging runtime to call `getSystemLoadMessage()` for every deserialized `SystemLoad` from Kafka. That method immediately delegates to `broadcastData()`, which builds an `OutboundSseEvent` with a named type (`"systemLoad"`), a JSON-serialized payload, and pushes it to all connected browsers via `broadcaster.broadcast()`.

</details>

---

**Q5. What is an `OutboundSseEvent` and how is it constructed?**

<details>
<summary>Answer</summary>

`OutboundSseEvent` is the JAX-RS representation of a single SSE event to be sent to a client. It is built using a fluent builder from `Sse`:

```java
OutboundSseEvent event = sse.newEventBuilder()
    .name("systemLoad")                        // SSE event type — browser listens on this name
    .data(data.getClass(), data)               // payload object + its Class for type-safe serialization
    .mediaType(MediaType.APPLICATION_JSON_TYPE) // serialize the payload as JSON
    .build();
```

The `name` field sets the `event:` field in the SSE wire format. The browser's `EventSource` fires listeners registered for that name, so `source.addEventListener('systemLoad', handler)` only triggers when an event with `event: systemLoad` is received.

</details>

---

**Q6. How does the frontend JavaScript subscribe to SSE events and update the DOM?**

<details>
<summary>Answer</summary>

`index.js` opens an `EventSource` connection on page load and registers a named listener:

```javascript
function initSSE() {
  var source = new EventSource("http://localhost:9084/bff/sse", {
    withCredentials: true,
  });
  source.addEventListener("systemLoad", systemLoadHandler);
}
```

`systemLoadHandler` parses the JSON payload and either updates an existing table row or creates a new one:

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

The hostname doubles as the DOM element ID so repeated updates overwrite the existing row rather than appending duplicates. `{ withCredentials: true }` is required because the BFF's CORS configuration sets `allowCredentials="true"` on the `/bff/sse` path.

</details>

---

**Q7. Why does the BFF's `server.xml` need a CORS configuration, and what does it allow?**

<details>
<summary>Answer</summary>

The Frontend (port 9080) and the BFF (port 9084) run on different origins. Without a CORS policy, the browser would block the `EventSource` request from `localhost:9080` to `localhost:9084`. The BFF's `server.xml` explicitly permits this:

```xml
<cors domain="/bff/sse"
    allowedOrigins="*"
    allowedMethods="GET"
    allowedHeaders="accept"
    allowCredentials="true"
    maxAge="3600" />
```

`allowCredentials="true"` is necessary because the `EventSource` is constructed with `{ withCredentials: true }` in the JavaScript. Without both sides agreeing on credentials, the browser rejects the CORS preflight even if `allowedOrigins="*"` is set.

</details>

---

**Q8. What is the `UPDATE_INTERVAL` config property and what is its default value?**

<details>
<summary>Answer</summary>

`UPDATE_INTERVAL` controls how frequently the System Microservice publishes a `SystemLoad` event to Kafka:

```java
@Inject
@ConfigProperty(name = "UPDATE_INTERVAL", defaultValue = "5")
private long updateInterval;

@Outgoing("systemLoad")
public Publisher<SystemLoad> sendSystemLoad() {
    return Flowable.interval(updateInterval, TimeUnit.SECONDS)
                   .map(interval -> new SystemLoad(getHostname(),
                         OS_MEAN.getSystemLoadAverage()));
}
```

The default is **5 seconds**. Because it is injected via `@ConfigProperty`, the interval can be overridden per-environment using any MicroProfile Config source — for example, passing an environment variable to the container — without changing or recompiling the Java source.

</details>

---

**Q9. What is the relationship between the `@Incoming("systemLoad")` channel name and the `system.load` Kafka topic?**

<details>
<summary>Answer</summary>

They are separate. The annotation value (`"systemLoad"`) is a **logical channel name** used inside the BFF application. The physical **Kafka topic name** (`system.load`) is declared in `microprofile-config.properties`:

```properties
# BFF Microservice — consumer
mp.messaging.incoming.systemLoad.topic=system.load

# System Microservice — producer (same physical topic)
mp.messaging.outgoing.systemLoad.topic=system.load
```

This separation means the topic name can be changed per-environment without touching Java source. Both services coincidentally use the same logical channel name (`systemLoad`), but only the physical topic name must match.

</details>

---

**Q10. What is the consumer `group.id` for the BFF, and why does it matter for the integration test?**

<details>
<summary>Answer</summary>

The BFF uses `group.id=bff`:

```properties
mp.messaging.incoming.systemLoad.group.id=bff
```

The integration test creates its own `KafkaConsumer` with `group.id=system-load-status`:

```java
consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "system-load-status");
```

Because they use different consumer groups, Kafka delivers each message to **both** groups independently. If the test reused the BFF's `group.id`, it would compete with the BFF for partition ownership and might receive no messages at all. Separate group IDs let the test observe the same Kafka messages the BFF would receive, in parallel, without interference.

</details>

---

**Q11. How does the integration test verify that the System Microservice is publishing valid messages?**

<details>
<summary>Answer</summary>

The test creates a `KafkaConsumer` subscribed to `system.load` with `AUTO_OFFSET_RESET_CONFIG=earliest` and polls for up to 60 seconds:

```java
@Test
public void testCpuStatus() {
    ConsumerRecords<String, SystemLoad> records =
        consumer.poll(Duration.ofMillis(60 * 1000));

    for (ConsumerRecord<String, SystemLoad> record : records) {
        SystemLoad sl = record.value();
        assertNotNull(sl.hostname);
        assertNotNull(sl.loadAverage);
    }
    consumer.commitAsync();
}
```

At the System Microservice's default 5-second interval, the 60-second poll window is long enough to receive multiple messages. The test asserts that each deserialized `SystemLoad` has non-null `hostname` and `loadAverage` fields. By using `SystemLoadDeserializer`, the test exercises the full serialization round-trip — it does not bypass deserialization with a raw `StringDeserializer`. The BFF and Frontend are not required to be running.

</details>

---

**Q12. What would happen if `broadcastData()` were called before any browser client connected to the SSE endpoint?**

<details>
<summary>Answer</summary>

`broadcaster` is initialized lazily — it is only created when the first browser client calls `GET /bff/sse/`. If a Kafka message arrives before any client connects, `broadcaster` is still `null`, and the null-guard in `broadcastData()` prevents a `NullPointerException`:

```java
private void broadcastData(String name, Object data) {
    if (broadcaster != null) {
        // ... build and broadcast event
    } else {
        logger.info("Unable to send SSE. Broadcaster context is not set up.");
    }
}
```

The message is silently dropped with a log warning. This is intentional — SSE is a push-only protocol with no persistence; missed events before a client connects are not replayed. If guaranteed delivery of historical events were required, a different pattern (e.g., storing events and replaying on connect) would be needed.

</details>
