package com.example;

import com.google.gson.Gson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

class GroupIntoBatchesKafkaIO {

    /**
     * Represents a single click event with user ID, event type, metadata, and timestamp.
     * This class must be Serializable for Apache Beam to process it.
     */
    public static class ClickEvent implements Serializable {
        private String userId;
        private String eventType;
        private String metadata; // New field for additional data
        private long timestamp; // Event timestamp in milliseconds (epoch)
        private String eventData;

        // Default constructor is required for Beam's serialization/deserialization
        public ClickEvent() {}

        public ClickEvent(String userId, String eventType, String metadata, long timestamp, String eventData) {
            this.userId = userId;
            this.eventType = eventType;
            this.metadata = metadata;
            this.timestamp = timestamp;
            this.eventData = eventData;
        }

        // Getters for Beam to access properties
        public String getUserId() {
            return userId;
        }

        public String getEventType() {
            return eventType;
        }

        public String getMetadata() { // Getter for the new metadata field
            return metadata;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public String getEventData() {
            return eventData;
        }

        @Override
        public String toString() {
            return "ClickEvent{" +
                    "userId='" + userId + '\'' +
                    ", eventType='" + eventType + '\'' +
                    ", metadata='" + metadata + '\'' +
                    ", timestamp=" + new Instant(timestamp) +
                    ", eventData='" + eventData + '\'' +
                    '}';
        }
    }

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        // Define the Kafka topic and bootstrap servers
        final String kafkaBootstrapServers = "localhost:9092"; // Your Kafka broker address
        final String kafkaTopic = "clickstream-events"; // The topic you created

        // REMOVED: final Gson gson = new Gson(); // Gson is now instantiated inside the lambda

        // Read from Kafka
        PCollection<KV<String, String>> kafkaRawEvents = p.apply("ReadFromKafka", KafkaIO.<String, String>read()
                        .withBootstrapServers(kafkaBootstrapServers)
                        .withTopic(kafkaTopic)
                        .withKeyDeserializer(StringDeserializer.class) // Assuming Kafka key is String
                        .withValueDeserializer(StringDeserializer.class) // Assuming Kafka value is JSON String
                        .withoutMetadata() // Read only KV<Key, Value>
                // For streaming, you typically want to start from the beginning for testing
                // .withConsumerConfigUpdates(Map.of("auto.offset.reset", "earliest")) // Uncomment to read from beginning
        );

        // Parse Kafka message values (JSON strings) into ClickEvent objects
        PCollection<ClickEvent> parsedEvents = kafkaRawEvents
                .apply("ParseKafkaMessage", MapElements.into(TypeDescriptor.of(ClickEvent.class))
                        .via(kv -> {
                            // NEW: Instantiate Gson inside the lambda to ensure serializability
                            final Gson gson = new Gson();
                            try {
                                // Deserialize the JSON string value into a ClickEvent object
                                return gson.fromJson(kv.getValue(), ClickEvent.class);
                            } catch (Exception e) {
                                // Log parsing errors and potentially output to a dead-letter queue
                                LoggerFactory.getLogger(GroupIntoBatchesKafkaIO.class).error("Failed to parse Kafka message: " + kv.getValue(), e);
                                return null; // Or throw an exception, or return a default/error event
                            }
                        })
                )
                .apply("FilterNulls", ParDo.of(new DoFn<ClickEvent, ClickEvent>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        if (c.element() != null) {
                            c.output(c.element());
                        }
                    }
                })); // Filter out any nulls from parsing errors

        // Assign event timestamps and key by userId
        PCollection<KV<String, ClickEvent>> keyedEvents = parsedEvents
                .apply("AssignEventTimestamps", WithTimestamps.of((ClickEvent event) -> new Instant(event.getTimestamp())))
                .apply("KeyByUserId", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(ClickEvent.class)))
                        .via(event -> KV.of(event.getUserId(), event)));

        // Apply GroupIntoBatches transformation
        PCollection<KV<String, Iterable<ClickEvent>>> batched =
                keyedEvents.apply("GroupIntoBatches",
                        GroupIntoBatches.<String, ClickEvent>ofSize(3)
                                .withMaxBufferingDuration(Duration.standardMinutes(2)));

        // Print results using the custom DoFn
        batched.apply("PrintBatches", ParDo.of(new PrintBatchesFn()));

        // Run the pipeline
        p.run().waitUntilFinish();
    }

    /**
     * A DoFn to print the contents of each flushed batch in a minimal format.
     * Now accepts KV<String, Iterable<ClickEvent>> as input.
     */
    public static class PrintBatchesFn extends DoFn<KV<String, Iterable<ClickEvent>>, Void> {
        private static final Logger LOG = LoggerFactory.getLogger(PrintBatchesFn.class);
        private static final AtomicLong batchCounter = new AtomicLong(0);
        private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("HH:mm:ss"); // For event timestamps

        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow window) {
            String user = c.element().getKey();
            Iterable<ClickEvent> events = c.element().getValue();
            long currentBatchId = batchCounter.incrementAndGet();

            List<ClickEvent> eventList = StreamSupport.stream(events.spliterator(), false)
                    .collect(Collectors.toList());

            StringBuilder sb = new StringBuilder();
            sb.append(String.format("Batch %d (User: %s, Total: %d events)\n",
                    currentBatchId, user, eventList.size()));
            sb.append("  Events:\n");
            for (ClickEvent event : eventList) {
                sb.append(String.format("    - [TS: %s] Type: %s, Metadata: '%s', Data: '%s'\n",
                        FORMATTER.print(new Instant(event.getTimestamp())),
                        event.getEventType(),
                        event.getMetadata(),
                        event.getEventData()));
            }

            if (window instanceof IntervalWindow) {
                sb.append("  Window: Interval\n");
            } else {
                sb.append("  Window: Global\n");
            }
            sb.append("---\n"); // Separator for readability

            LOG.info(sb.toString());
        }
    }
}

//{"userId":"userA","eventType":"page_view","metadata":"{\"browser\":\"Chrome\"}","timestamp":1755782823284,"eventData":"/home"}
//{"userId":"userA","eventType":"click","metadata":"{\"element\":\"button\"}","timestamp":1755782833183,"eventData":"add_to_cart"}
//{"userId":"userA","eventType":"scroll","metadata":"{\"depth\":0.5}","timestamp":1755782843250,"eventData":"product_page"}
//
//{"userId":"userA","eventType":"purchase","metadata":"{\"amount\":100}","timestamp":1755782854610,"eventData":"item_id_123"}
//
//{"userId":"userB","eventType":"login","metadata":"{\"method\":\"email\"}","timestamp":1755783052517,"eventData":"success"}
//{"userId":"userB","eventType":"click","metadata":"{\"element\":\"button\"}","timestamp":1755783172320,"eventData":"add_to_cart"}
//{"userId":"userB","eventType":"click","metadata":"{\"element\":\"foo\"}","timestamp":1755783229773,"eventData":"bar"}
//{"userId":"userB","eventType":"click","metadata":"{\"element\":\"abc\"}","timestamp":1755783246867,"eventData":"efg"}
//{"userId":"userB","eventType":"click","metadata":"{\"element\":\"ldd\"}","timestamp":1755783266498,"eventData":"fff"}
