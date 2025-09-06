package com.example;

import com.google.gson.Gson;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.TimeoutException;

public final class KafkaToRestApiSparkJob {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaToRestApiSparkJob.class);

    // This is the same as the ClickEvent class from your Beam code
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
                    ", timestamp=" + new java.util.Date(timestamp) +
                    ", eventData='" + eventData + '\'' +
                    '}';
        }
    }

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        final String kafkaBootstrapServers = "broker:29092"; // Corrected port
        final String kafkaTopic = "clickstream-events";
        final String apiEndpoint = "http://host.docker.internal:5000/api/data";

        SparkSession spark = SparkSession
                .builder()
                .appName("KafkaToRestApiSparkJob")
                .getOrCreate();

        // 1. Define the schema for the Kafka message payload
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("userId", DataTypes.StringType, true),
                DataTypes.createStructField("eventType", DataTypes.StringType, true),
                DataTypes.createStructField("metadata", DataTypes.StringType, true),
                DataTypes.createStructField("timestamp", DataTypes.LongType, true),
                DataTypes.createStructField("eventData", DataTypes.StringType, true)
        });

        // 2. Read from Kafka
        Dataset<Row> rawEvents = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                .option("subscribe", kafkaTopic)
                .load();

        // 3. Parse Kafka message values (JSON strings) into a structured DataFrame
        Dataset<Row> parsedEvents = rawEvents
                .selectExpr("CAST(value AS STRING) as json")
                .select(org.apache.spark.sql.functions.from_json(
                        org.apache.spark.sql.functions.col("json"), schema).as("data"))
                .select("data.*");

        // 4. Repartition the data before sending it to the API
        Dataset<Row> repartitionedEvents = parsedEvents.repartition(20);

        // 5. Send data to the REST API using foreachBatch
        StreamingQuery query = repartitionedEvents
                .writeStream()
                .foreachBatch((batchDf, batchId) -> {
                    LOG.info("Processing micro-batch {}", batchId);

                    batchDf.foreachPartition((ForeachPartitionFunction<Row>) partition -> {
                        final Gson gson = new Gson();
                        HttpClient httpClient = HttpClient.newBuilder().build();

                        while (partition.hasNext()) {
                            Row row = partition.next();

                            // Safely retrieve values, handling nulls
                            String userId = row.isNullAt(0) ? null : row.getString(0);
                            String eventType = row.isNullAt(1) ? null : row.getString(1);
                            String metadata = row.isNullAt(2) ? null : row.getString(2);
                            Long timestamp = row.isNullAt(3) ? null : row.getLong(3);
                            String eventData = row.isNullAt(4) ? null : row.getString(4);

                            // Check for a minimum set of non-null fields
                            if (userId == null || eventType == null) {
                                LOG.warn("Skipping malformed row: missing userId or eventType.");
                                continue;
                            }

                            ClickEvent event = new ClickEvent(
                                    userId,
                                    eventType,
                                    metadata,
                                    timestamp != null ? timestamp : -1L, // Provide a default for timestamp
                                    eventData
                            );
                            String jsonPayload = gson.toJson(event);

                            try {
                                HttpRequest request = HttpRequest.newBuilder()
                                        .uri(URI.create(apiEndpoint))
                                        .header("Content-Type", "application/json")
                                        .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                                        .build();

                                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                                LOG.info("API call successful for event from user '{}'. Status: {}, Body: {}",
                                        event.getUserId(), response.statusCode(), response.body());
                            } catch (Exception e) {
                                System.out.println("Api Call Failed" + e.getMessage());
                                LOG.error("Failed to send event to API for user '{}'", event.getUserId(), e);
                            }
                        }
                    });
                })
                .start();

        query.awaitTermination();
    }
}
