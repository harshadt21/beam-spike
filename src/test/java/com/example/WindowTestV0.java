package com.example.beam;

import com.example.Event;
import com.example.UserSessionDoFn;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

import java.util.UUID;


class PrintSessionsFn extends DoFn<KV<String, Iterable<Event>>, Void> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        Iterable<Event> events = c.element().getValue();
        Instant windowStart = null;
        Instant windowEnd = null;

        for (Event e : events) {
            if (windowStart == null || e.timestamp.isBefore(windowStart)) {
                windowStart = e.timestamp;
            }
            if (windowEnd == null || e.timestamp.isAfter(windowEnd)) {
                windowEnd = e.timestamp;
            }
        }

        String output = "";

        output += "User=" + c.element().getKey() +  " | Window=[" + windowStart + " - " + windowEnd + "]\n";
        for (Event e : events) {
            output += "     ";
            output += e;
            output +=  "\n";
        }
        System.out.println(output);
    }
}

// -------------------- WindowTest --------------------
public class WindowTestV0 {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testUserSessionsAllScenarios() {

        Instant start = Instant.parse("2025-01-01T00:00:00Z");

        TestStream<Event> topicAStream = TestStream.create(SerializableCoder.of(Event.class))
                .advanceWatermarkTo(start)

                // -------- User A Scenario --------
                // Window 1: 3 events quickly
                .addElements(
                        TimestampedValue.of(new Event("userA", UUID.randomUUID().toString(), "m1",
                                start.plus(Duration.standardSeconds(5))), start.plus(Duration.standardSeconds(5))),
                        TimestampedValue.of(new Event("userA", UUID.randomUUID().toString(), "m3",
                                start.plus(Duration.standardSeconds(45))), start.plus(Duration.standardSeconds(45))),
                        TimestampedValue.of(new Event("userB", UUID.randomUUID().toString(), "m2",
                                start.plus(Duration.standardSeconds(20))), start.plus(Duration.standardSeconds(20))),
                        TimestampedValue.of(new Event("userB", UUID.randomUUID().toString(), "m3",
                                start.plus(Duration.standardSeconds(45))), start.plus(Duration.standardSeconds(45)))
                )
                .advanceWatermarkTo(start.plus(Duration.standardMinutes(1)).plus(Duration.standardSeconds(1)))
                // Window 2: 2 events
                .addElements(
                        TimestampedValue.of(new Event("userA", UUID.randomUUID().toString(), "m4",
                                start.plus(Duration.standardMinutes(2))), start.plus(Duration.standardMinutes(2))),
                        TimestampedValue.of(new Event("userA", UUID.randomUUID().toString(), "m5",
                                        start.plus(Duration.standardMinutes(3)).plus(Duration.standardSeconds(30))),
                                start.plus(Duration.standardMinutes(3)).plus(Duration.standardSeconds(30)))
                )
                .advanceWatermarkTo(start.plus(Duration.standardMinutes(5)).plus(Duration.standardSeconds(1)))
                // Window 3: 1 event
                .addElements(
                        TimestampedValue.of(new Event("userA", UUID.randomUUID().toString(), "m6",
                                start.plus(Duration.standardMinutes(5))), start.plus(Duration.standardMinutes(5)))
                )
                .advanceWatermarkToInfinity();

        TestStream<Event> topicBStream = TestStream.create(SerializableCoder.of(Event.class))
                .advanceWatermarkTo(start)
                .addElements(
                        TimestampedValue.of(new Event("userA", UUID.randomUUID().toString(), "m2",
                                start.plus(Duration.standardSeconds(20))), start.plus(Duration.standardSeconds(20))),
                        TimestampedValue.of(new Event("userB", UUID.randomUUID().toString(), "m1",
                                start.plus(Duration.standardSeconds(5))), start.plus(Duration.standardSeconds(5)))
                )
                .advanceWatermarkToInfinity();

//        TestStream<Event> topicCStream = TestStream.create(SerializableCoder.of(Event.class))
//                .advanceWatermarkTo(start.plus(Duration.standardMinutes(5)).plus(Duration.standardSeconds(0)))
//                .addElements(
//                        TimestampedValue.of(new Event("userA", UUID.randomUUID().toString(), "m6",
//                                start.plus(Duration.standardMinutes(5))), start.plus(Duration.standardMinutes(5)))
//                )
//                .advanceWatermarkToInfinity();

//        TestStream<Event> topicCStream = TestStream.create(SerializableCoder.of(Event.class))
//                .advanceWatermarkTo(start.plus(Duration.standardMinutes(5)).plus(Duration.standardSeconds(0)))
//                .addElements(
//                        TimestampedValue.of(new Event("userA", UUID.randomUUID().toString(), "m6",
//                                start.plus(Duration.standardMinutes(5))), start.plus(Duration.standardMinutes(5)))
//                )
//                .advanceWatermarkToInfinity();

        PCollection<Event> allEvents = PCollectionList.of(pipeline.apply("TopicA", topicAStream))
                .and(pipeline.apply("TopicB", topicBStream))
//                .and(pipeline.apply("TopicC", topicCStream))
                .apply("MergeTopics", Flatten.pCollections());


        allEvents
                .apply("KeyByUser", WithKeys.of((Event e) -> e.userId))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Event.class)))
                .apply("UserSessions", ParDo.of(new UserSessionDoFn()))
                .apply("PrintSessions", ParDo.of(new PrintSessionsFn()));

        pipeline.run().waitUntilFinish();
    }
}

// output

//User=userB | Window=[2025-01-01T00:00:05.000Z - 2025-01-01T00:00:45.000Z]
//  Event{userId='userB', eventId='53da1c30-16f9-458d-a42d-2f9a3854ff3f', metadata='m1', timestamp=2025-01-01T00:00:05.000Z}
//  Event{userId='userB', eventId='8543b2d3-b79c-4379-8c4b-91879622622f', metadata='m2', timestamp=2025-01-01T00:00:20.000Z}
//  Event{userId='userB', eventId='2a15111f-5b9e-46d8-9a32-608ce0816f45', metadata='m3', timestamp=2025-01-01T00:00:45.000Z}
//
//User=userA | Window=[2025-01-01T00:00:05.000Z - 2025-01-01T00:00:45.000Z]
//  Event{userId='userA', eventId='66ad0694-ad4f-4806-8229-2821fb76d2e9', metadata='m1', timestamp=2025-01-01T00:00:05.000Z}
//  Event{userId='userA', eventId='2152531e-f281-4618-a74e-681f0daa26bd', metadata='m2', timestamp=2025-01-01T00:00:20.000Z}
//  Event{userId='userA', eventId='9e67cce5-14e8-435b-9723-5d02228e82c7', metadata='m3', timestamp=2025-01-01T00:00:45.000Z}
//
//User=userA | Window=[2025-01-01T00:02:00.000Z - 2025-01-01T00:03:30.000Z]
//  Event{userId='userA', eventId='ec581757-17c4-464c-82c9-0bfe84190df0', metadata='m4', timestamp=2025-01-01T00:02:00.000Z}
//  Event{userId='userA', eventId='752f2cdb-faac-4fda-829a-71e99b079626', metadata='m5', timestamp=2025-01-01T00:03:30.000Z}
//
//User=userA | Window=[2025-01-01T00:05:00.000Z - 2025-01-01T00:05:00.000Z]
//  Event{userId='userA', eventId='bf24e845-a38a-44e6-b929-99fab0c96ac4', metadata='m6', timestamp=2025-01-01T00:05:00.000Z}







//                // -------- User B Scenario --------
//                // Each event in separate window
//                .addElements(
//                        TimestampedValue.of(new Event("userB", UUID.randomUUID().toString(), "m1",
//                                start.plus(Duration.standardSeconds(2))), start.plus(Duration.standardSeconds(2))),
//                        TimestampedValue.of(new Event("userB", UUID.randomUUID().toString(), "m2",
//                                start.plus(Duration.standardMinutes(1))), start.plus(Duration.standardMinutes(1))),
//                        TimestampedValue.of(new Event("userB", UUID.randomUUID().toString(), "m3",
//                                start.plus(Duration.standardMinutes(2))), start.plus(Duration.standardMinutes(2)))
//                )
//                // Next window: 3 events quickly
//                .addElements(
//                        TimestampedValue.of(new Event("userB", UUID.randomUUID().toString(), "m4",
//                                start.plus(Duration.standardMinutes(3))), start.plus(Duration.standardMinutes(3))),
//                        TimestampedValue.of(new Event("userB", UUID.randomUUID().toString(), "m5",
//                                        start.plus(Duration.standardMinutes(3)).plus(Duration.standardSeconds(10))),
//                                start.plus(Duration.standardMinutes(3)).plus(Duration.standardSeconds(10))),
//                        TimestampedValue.of(new Event("userB", UUID.randomUUID().toString(), "m6",
//                                        start.plus(Duration.standardMinutes(3)).plus(Duration.standardSeconds(20))),
//                                start.plus(Duration.standardMinutes(3)).plus(Duration.standardSeconds(20)))
//                )
//
//                // -------- User C Scenario --------
//                // Mixed: 1 event, then 3 quickly (MAX_EVENTS triggers), then 1 late
//                .addElements(
//                        TimestampedValue.of(new Event("userC", UUID.randomUUID().toString(), "m1",
//                                start.plus(Duration.standardSeconds(1))), start.plus(Duration.standardSeconds(1))),
//                        TimestampedValue.of(new Event("userC", UUID.randomUUID().toString(), "m2",
//                                start.plus(Duration.standardMinutes(1))), start.plus(Duration.standardMinutes(1))),
//                        TimestampedValue.of(new Event("userC", UUID.randomUUID().toString(), "m3",
//                                        start.plus(Duration.standardMinutes(1)).plus(Duration.standardSeconds(5))),
//                                start.plus(Duration.standardMinutes(1)).plus(Duration.standardSeconds(5))),
//                        TimestampedValue.of(new Event("userC", UUID.randomUUID().toString(), "m4",
//                                        start.plus(Duration.standardMinutes(1)).plus(Duration.standardSeconds(10))),
//                                start.plus(Duration.standardMinutes(1)).plus(Duration.standardSeconds(10))),
//                        TimestampedValue.of(new Event("userC", UUID.randomUUID().toString(), "m5",
//                                start.plus(Duration.standardMinutes(3))), start.plus(Duration.standardMinutes(3)))
//                )

