package com.example;

import com.google.gson.Gson;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class GroupIntoBatchesKafkaIOV1 {

    public static class ClickEvent implements Serializable {
        private String userId;
        private String eventType;
        private String metadata;
        private long timestamp;
        private String eventData;

        public ClickEvent() {
        }

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
        final String apiEndpoint = "http://host.docker.internal:5000/api/data";


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

        PCollection<KV<String, ClickEvent>> keyedEvents = parsedEvents
                .apply("AssignEventTimestamps", WithTimestamps.of((ClickEvent event) -> new Instant(event.getTimestamp())))
                .apply("KeyByUserId", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(ClickEvent.class)))
                        .via(event -> KV.of(event.getUserId(), event)));

        PCollection<KV<String, Iterable<ClickEvent>>> batched =
                keyedEvents.apply("GroupIntoBatches",
                        GroupIntoBatches.<String, ClickEvent>ofSize(100)
                                .withMaxBufferingDuration(Duration.standardMinutes(1)));

        batched.apply("SendIndividualEventsInBatchToAPI", ParDo.of(new ApiCallBatchElementsFn(apiEndpoint)));

        p.run().waitUntilFinish();
    }

    /**
     * A DoFn to format and send a single event to an external API.
     */
    public static class ApiCallBatchElementsFn extends DoFn<KV<String, Iterable<ClickEvent>>, Void> {
        private static final Logger LOG = LoggerFactory.getLogger(ApiCallBatchElementsFn.class);
        private static final Gson GSON = new Gson();

        private final String apiEndpoint;
        private transient HttpClient httpClient;

        public ApiCallBatchElementsFn(String apiEndpoint) {
            this.apiEndpoint = apiEndpoint;
        }

        @Setup
        public void setup() {
            this.httpClient = HttpClient.newHttpClient();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String userId = c.element().getKey();
            Iterable<ClickEvent> events = c.element().getValue();

            // Iterate over each event in the batched iterable
            for (ClickEvent event : events) {
                String jsonPayload = GSON.toJson(event);

                try {
                    // Build the HTTP request for a single event
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create(apiEndpoint))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                            .build();

                    // Send the request and get the response
                    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                    // Log the API response for the individual event
                    LOG.info("API call successful for event from user '{}'. Status: {}, Body: {}",
                            userId, response.statusCode(), response.body());

                } catch (Exception e) {
                    LOG.error("Failed to send event to API for user '{}'", userId, e);
                }
            }
        }
    }
}
// Input events for Kafka
//{"userId":"userA","eventType":"page_view","metadata":"{\"browser\":\"Chrome\"}","timestamp":1756808417117,"eventData":"/home"}
//{"userId":"userA","eventType":"click","metadata":"{\"element\":\"button\"}","timestamp":1756808428315,"eventData":"add_to_cart"}
//{"userId":"userA","eventType":"scroll","metadata":"{\"depth\":0.5}","timestamp":1756808439597,"eventData":"product_page"}
//
//{"userId":"userA","eventType":"purchase","metadata":"{\"amount\":100}","timestamp":1756808488227,"eventData":"item_id_123"}
//
//{"userId":"userB","eventType":"login","metadata":"{\"method\":\"email\"}","timestamp":1756808063956,"eventData":"success"}
//{"userId":"userB","eventType":"click","metadata":"{\"element\":\"button\"}","timestamp":1756808092479,"eventData":"add_to_cart"}
//{"userId":"userB","eventType":"click","metadata":"{\"element\":\"foo\"}","timestamp":1756808117679,"eventData":"bar"}
//{"userId":"userB","eventType":"click","metadata":"{\"element\":\"abc\"}","timestamp":1756808129690,"eventData":"efg"}
//{"userId":"userB","eventType":"click","metadata":"{\"element\":\"ldd\"}","timestamp":1765783266498,"eventData":"fff"}
