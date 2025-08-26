package com.example;

import com.google.gson.Gson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
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

public class GroupIntoBatchesKafkaIO {

    public static class ClickEvent implements Serializable {
        private String userId;
        private String eventType;
        private String metadata;
        private long timestamp;
        private String eventData;

        public ClickEvent() {}

        public ClickEvent(String userId, String eventType, String metadata, long timestamp, String eventData) {
            this.userId = userId;
            this.eventType = eventType;
            this.metadata = metadata;
            this.timestamp = timestamp;
            this.eventData = eventData;
        }

        public String getUserId() {
            return userId;
        }

        public String getEventType() {
            return eventType;
        }

        public String getMetadata() {
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


        final String kafkaBootstrapServers = "broker:29092";
        final String kafkaTopic = "clickstream-events";


        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        options.as(FlinkPipelineOptions.class).setRunner(org.apache.beam.runners.flink.FlinkRunner.class);

        Pipeline p = Pipeline.create(options);

        // Read from Kafka
        PCollection<KV<String, String>> kafkaRawEvents = p.apply("ReadFromKafka", KafkaIO.<String, String>read()
                        .withBootstrapServers(kafkaBootstrapServers)
                        .withTopic(kafkaTopic)
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withoutMetadata()
        );

        // Parse Kafka message values (JSON strings) into ClickEvent objects
        PCollection<ClickEvent> parsedEvents = kafkaRawEvents
                .apply("ParseKafkaMessage", MapElements.into(TypeDescriptor.of(ClickEvent.class))
                        .via(kv -> {
                            final Gson gson = new Gson();
                            try {
                                return gson.fromJson(kv.getValue(), ClickEvent.class);
                            } catch (Exception e) {
                                LoggerFactory.getLogger(GroupIntoBatchesKafkaIO.class).error("Failed to parse Kafka message: " + kv.getValue(), e);
                                return null;
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
                }));

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
     * A DoFn to print the contents of each flushed batch
     */
    public static class PrintBatchesFn extends DoFn<KV<String, Iterable<ClickEvent>>, Void> {
        private static final Logger LOG = LoggerFactory.getLogger(PrintBatchesFn.class);
        private static final AtomicLong batchCounter = new AtomicLong(0);
        private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("HH:mm:ss");

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
            sb.append("---\n");

            LOG.info(sb.toString());
        }
    }
}
// Input events for Kafka
//{"userId":"userA","eventType":"page_view","metadata":"{\"browser\":\"Chrome\"}","timestamp":1756199813292,"eventData":"/home"}
//{"userId":"userA","eventType":"click","metadata":"{\"element\":\"button\"}","timestamp":1756199826249,"eventData":"add_to_cart"}
//{"userId":"userA","eventType":"scroll","metadata":"{\"depth\":0.5}","timestamp":1756199837848,"eventData":"product_page"}
//
//{"userId":"userA","eventType":"purchase","metadata":"{\"amount\":100}","timestamp":1756199854809,"eventData":"item_id_123"}
//
//{"userId":"userB","eventType":"login","metadata":"{\"method\":\"email\"}","timestamp":1756199379379,"eventData":"success"}
//{"userId":"userB","eventType":"click","metadata":"{\"element\":\"button\"}","timestamp":1756199390026,"eventData":"add_to_cart"}
//{"userId":"userB","eventType":"click","metadata":"{\"element\":\"foo\"}","timestamp":1755783229773,"eventData":"bar"}
//{"userId":"userB","eventType":"click","metadata":"{\"element\":\"abc\"}","timestamp":1755783246867,"eventData":"efg"}
//{"userId":"userB","eventType":"click","metadata":"{\"element\":\"ldd\"}","timestamp":1755783266498,"eventData":"fff"}
