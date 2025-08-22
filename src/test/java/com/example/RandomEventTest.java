package com.example;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

import java.util.Random;

public class RandomEventTest {

    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    private static final String[] USERS = {"userA", "userB", "userC"};
    private static final String[] TOPICS = {"topic1", "topic2", "topic3"};

    @Test
    public void testRandomEventsForTwoMinutes() {
        Random random = new Random(42); // deterministic for test reproducibility
        Instant start = new Instant(0);

        TestStream.Builder<String> builder =
                TestStream.create(org.apache.beam.sdk.coders.StringUtf8Coder.of());

        Instant current = start;
        Duration step = Duration.standardSeconds(10); // emit events every 10s

        while (current.isBefore(start.plus(Duration.standardMinutes(2)))) {
            // generate a random event
            String user = USERS[random.nextInt(USERS.length)];
            String topic = TOPICS[random.nextInt(TOPICS.length)];
            String event = user + "-" + topic;

            builder = builder
                    .advanceWatermarkTo(current)
                    .addElements(event);

            current = current.plus(step);
        }

        // Finish the stream
        TestStream<String> events = builder.advanceWatermarkToInfinity();

        // Apply the TestStream to pipeline
        PCollection<String> input = pipeline.apply(events);

        // Just map events to uppercase (you can replace with real transforms)
        PCollection<String> output =
                input.apply(MapElements
                        .into(TypeDescriptor.of(String.class))
                        .via((String e) -> "Processed-" + e));

        // Example check: ensure we saw some expected events
        PAssert.that(output)
                .containsInAnyOrder("Processed-userA-topic1",
                        "Processed-userB-topic2",
                        "Processed-userC-topic3");

        pipeline.run();
    }
}
