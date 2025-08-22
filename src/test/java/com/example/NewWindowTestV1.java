//package com.example;
//
//import org.apache.beam.sdk.coders.DefaultCoder;
//import org.apache.beam.sdk.coders.KvCoder;
//import org.apache.beam.sdk.coders.SerializableCoder;
//import org.apache.beam.sdk.coders.StringUtf8Coder;
//import org.apache.beam.sdk.testing.TestPipeline;
//import org.apache.beam.sdk.testing.TestStream;
//import org.apache.beam.sdk.transforms.DoFn;
//import org.apache.beam.sdk.transforms.PTransform;
//import org.apache.beam.sdk.transforms.ParDo;
//import org.apache.beam.sdk.transforms.WithKeys;
//import org.apache.beam.sdk.transforms.windowing.*;
//import org.apache.beam.sdk.values.KV;
//import org.apache.beam.sdk.values.PCollection;
//import org.apache.beam.sdk.values.POutput;
//import org.apache.beam.sdk.values.TimestampedValue;
//import org.joda.time.Duration;
//import org.joda.time.Instant;
//import org.junit.Rule;
//import org.junit.Test;
//
//import java.io.Serializable;
//import java.util.UUID;
//
//public class NewWindowTestV1 {
//
//    @DefaultCoder(SerializableCoder.class)
//    public static class Event implements Serializable {
//        public String userId;
//        public String eventId;
//        public String metadata;
//        public Instant timestamp;
//
//        public Event() {}
//
//        public Event(String userId, String eventId, String metadata, Instant timestamp) {
//            this.userId = userId;
//            this.eventId = eventId;
//            this.metadata = metadata;
//            this.timestamp = timestamp;
//        }
//
//        @Override
//        public String toString() {
//            return "Event{" +
//                    "userId='" + userId + '\'' +
//                    ", eventId='" + eventId + '\'' +
//                    ", metadata='" + metadata + '\'' +
//                    ", timestamp=" + timestamp +
//                    '}';
//        }
//    }
//
//    public static class PrintSessionsFn extends DoFn<KV<String, Iterable<Event>>, Void> {
//        @ProcessElement
//        public void process(@Element KV<String, Iterable<Event>> kv, BoundedWindow window) {
//            String windowInfo;
//            if (window instanceof IntervalWindow) {
//                IntervalWindow iw = (IntervalWindow) window;
//                windowInfo = "[" + iw.start() + " - " + iw.end() + "]";
//            } else {
//                windowInfo = "[GlobalWindow maxTimestamp=" + window.maxTimestamp() + "]";
//            }
//            System.out.println("User=" + kv.getKey() + " | Window=" + windowInfo);
//            for (Event e : kv.getValue()) {
//                System.out.println("  " + e);
//            }
//        }
//    }
//
//    @Rule
//    public final transient TestPipeline pipeline = TestPipeline.create();
//
//    @Test
//    public void testUserSessions() {
//
//        Instant start = Instant.parse("2025-01-01T00:00:00Z");
//
//        // ---------------- TestStream ----------------
//        TestStream<Event> events = TestStream.create(SerializableCoder.of(Event.class))
//                .advanceWatermarkTo(start)
//
//                // User A: first window (3 events quickly)
//                .addElements(
//                        TimestampedValue.of(new Event("userA", UUID.randomUUID().toString(), "m1",
//                                start.plus(Duration.standardSeconds(5))), start.plus(Duration.standardSeconds(5))),
//                        TimestampedValue.of(new Event("userA", UUID.randomUUID().toString(), "m2",
//                                start.plus(Duration.standardSeconds(10))), start.plus(Duration.standardSeconds(10))),
//                        TimestampedValue.of(new Event("userA", UUID.randomUUID().toString(), "m3",
//                                start.plus(Duration.standardSeconds(15))), start.plus(Duration.standardSeconds(15)))
//                )
//                .advanceProcessingTime(Duration.standardMinutes(1)) // simulate 1 min of processing time passing
//
//                // User A: second window (2 events)
//                .addElements(
//                        TimestampedValue.of(new Event("userA", UUID.randomUUID().toString(), "m4",
//                                        start.plus(Duration.standardMinutes(3).plus(Duration.standardSeconds(5)))),
//                                start.plus(Duration.standardMinutes(3).plus(Duration.standardSeconds(5)))),
//                        TimestampedValue.of(new Event("userA", UUID.randomUUID().toString(), "m5",
//                                        start.plus(Duration.standardMinutes(3).plus(Duration.standardSeconds(30)))),
//                                start.plus(Duration.standardMinutes(3).plus(Duration.standardSeconds(30))))
//                )
//                .advanceProcessingTime(Duration.standardMinutes(1)) // advance simulated processing time
//
//                // User A: third window (1 event)
//                .addElements(
//                        TimestampedValue.of(new Event("userA", UUID.randomUUID().toString(), "m6",
//                                        start.plus(Duration.standardMinutes(6).plus(Duration.standardSeconds(5)))),
//                                start.plus(Duration.standardMinutes(6).plus(Duration.standardSeconds(5))))
//                )
//                .advanceWatermarkToInfinity();
//
//        // ---------------- Pipeline ----------------
//        PCollection<Event> input = pipeline.apply(events);
//
//        input
//                .apply("KeyByUser", WithKeys.of((Event e) -> e.userId))
//                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Event.class)))
//                .apply("WindowPerUser",
//                        Window.<KV<String, Event>>into(FixedWindows.of(Duration.standardMinutes(3)))
//                                .triggering(Repeatedly.forever(
//                                        AfterFirst.of(
//                                                AfterPane.elementCountAtLeast(3),
//                                                AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(3))
//                                        )
//                                ))
//                                .discardingFiredPanes()
//                                .withAllowedLateness(Duration.ZERO)
//                )
//                .apply(new PTransform<PCollection<KV<String, Event>>, POutput>() {
//                    @Override
//                    public POutput expand(PCollection<KV<String, Event>> input) {
//                        return null;
//                    }
//                })
//                .apply("Print", ParDo.of(new PrintSessionsFn()));
//
//        pipeline.run().waitUntilFinish();
//    }
//}
