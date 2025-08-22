package com.example.beam;

import com.example.Event;
import com.example.UserSessionDoFn;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

import java.util.UUID;

public class NewWindowTest {


    public static class PrintSessionsFn extends DoFn<KV<String, Iterable<Event>>, Void> {

        @ProcessElement
        public void process(@Element KV<String, Iterable<Event>> kv,
                            BoundedWindow window) {

            String windowInfo;

            if (window instanceof IntervalWindow) {
                IntervalWindow iw = (IntervalWindow) window;
                windowInfo = "[" + iw.start() + " - " + iw.end() + "]";
            } else if (window instanceof GlobalWindow) {
                GlobalWindow gw = (GlobalWindow) window;
                // For GlobalWindow, you can print maxTimestamp as a reference
                windowInfo = "[GlobalWindow maxTimestamp=" + gw.maxTimestamp() + "]";
            } else {
                windowInfo = "[Unknown window type: " + window.getClass().getSimpleName() + "]";
            }

            System.out.println("User=" + kv.getKey() + " | Window=" + windowInfo);

            for (Event e : kv.getValue()) {
                System.out.println("  " + e);
            }
        }
    }

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testUserSessions() {

        Instant start = Instant.parse("2025-01-01T00:00:00Z");


        TestStream<Event> events = TestStream.create(SerializableCoder.of(Event.class))
                .advanceWatermarkTo(start)

                // -------- User A: first window (3 events quickly) --------
                .addElements(
                        TimestampedValue.of(
                                new Event("userA", UUID.randomUUID().toString(), "m1",
                                        start.plus(Duration.standardSeconds(5))),
                                start.plus(Duration.standardSeconds(5))
                        ),
                        TimestampedValue.of(
                                new Event("userA", UUID.randomUUID().toString(), "m2",
                                        start.plus(Duration.standardSeconds(10))),
                                start.plus(Duration.standardSeconds(10))
                        ),
                        TimestampedValue.of(
                                new Event("userA", UUID.randomUUID().toString(), "m3",
                                        start.plus(Duration.standardSeconds(15))),
                                start.plus(Duration.standardSeconds(15))
                        )
                )

                // User A: second window (2 events)
                .addElements(
                        TimestampedValue.of(
                                new Event("userA", UUID.randomUUID().toString(), "m4",
                                        start.plus(Duration.standardMinutes(3).plus(Duration.standardSeconds(5)))),
                                start.plus(Duration.standardMinutes(3).plus(Duration.standardSeconds(5)))
                        ),
                        TimestampedValue.of(
                                new Event("userA", UUID.randomUUID().toString(), "m5",
                                        start.plus(Duration.standardMinutes(3).plus(Duration.standardSeconds(30)))),
                                start.plus(Duration.standardMinutes(3).plus(Duration.standardSeconds(30)))
                        )
                )

                // User A: third window (1 event)
                .addElements(
                        TimestampedValue.of(
                                new Event("userA", UUID.randomUUID().toString(), "m6",
                                        start.plus(Duration.standardMinutes(6).plus(Duration.standardSeconds(5)))),
                                start.plus(Duration.standardMinutes(6).plus(Duration.standardSeconds(5)))
                        )
                )

                // -------- User B: separate windows then 3 together --------
                .addElements(
                        TimestampedValue.of(
                                new Event("userB", UUID.randomUUID().toString(), "m1",
                                        start.plus(Duration.standardSeconds(2))),
                                start.plus(Duration.standardSeconds(2))
                        ),
                        TimestampedValue.of(
                                new Event("userB", UUID.randomUUID().toString(), "m2",
                                        start.plus(Duration.standardMinutes(1))),
                                start.plus(Duration.standardMinutes(1))
                        ),
                        TimestampedValue.of(
                                new Event("userB", UUID.randomUUID().toString(), "m3",
                                        start.plus(Duration.standardMinutes(2))),
                                start.plus(Duration.standardMinutes(2))
                        )
                )
                .addElements(
                        TimestampedValue.of(
                                new Event("userB", UUID.randomUUID().toString(), "m4",
                                        start.plus(Duration.standardMinutes(5).plus(Duration.standardSeconds(10)))),
                                start.plus(Duration.standardMinutes(5).plus(Duration.standardSeconds(10)))
                        ),
                        TimestampedValue.of(
                                new Event("userB", UUID.randomUUID().toString(), "m5",
                                        start.plus(Duration.standardMinutes(5).plus(Duration.standardSeconds(20)))),
                                start.plus(Duration.standardMinutes(5).plus(Duration.standardSeconds(20)))
                        ),
                        TimestampedValue.of(
                                new Event("userB", UUID.randomUUID().toString(), "m6",
                                        start.plus(Duration.standardMinutes(5).plus(Duration.standardSeconds(30)))),
                                start.plus(Duration.standardMinutes(5).plus(Duration.standardSeconds(30)))
                        )
                )

                // -------- User C: mixed scenario --------
                .addElements(
                        TimestampedValue.of(
                                new Event("userC", UUID.randomUUID().toString(), "m1",
                                        start.plus(Duration.standardSeconds(1))),
                                start.plus(Duration.standardSeconds(1))
                        ),
                        TimestampedValue.of(
                                new Event("userC", UUID.randomUUID().toString(), "m2",
                                        start.plus(Duration.standardMinutes(1))),
                                start.plus(Duration.standardMinutes(1))
                        ),
                        TimestampedValue.of(
                                new Event("userC", UUID.randomUUID().toString(), "m3",
                                        start.plus(Duration.standardMinutes(1).plus(Duration.standardSeconds(5)))),
                                start.plus(Duration.standardMinutes(1).plus(Duration.standardSeconds(5)))
                        ),
                        TimestampedValue.of(
                                new Event("userC", UUID.randomUUID().toString(), "m4",
                                        start.plus(Duration.standardMinutes(1).plus(Duration.standardSeconds(10)))),
                                start.plus(Duration.standardMinutes(1).plus(Duration.standardSeconds(10)))
                        ),
                        TimestampedValue.of(
                                new Event("userC", UUID.randomUUID().toString(), "m5",
                                        start.plus(Duration.standardMinutes(3))),
                                start.plus(Duration.standardMinutes(3))
                        )
                )

                .advanceWatermarkToInfinity();


        // Apply TestStream
        PCollection<Event> input = pipeline.apply(events);

        input
                .apply("KeyByUser", WithKeys.of((Event e) -> e.userId))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Event.class)))
                .apply("UserSessions", ParDo.of(new UserSessionDoFn()))
                .apply("PrintSessions", ParDo.of(new PrintSessionsFn()));

        pipeline.run().waitUntilFinish();
    }
}
