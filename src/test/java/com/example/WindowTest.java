package com.example;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

import java.io.Serializable;
import java.util.UUID;

public class WindowTest {

    @DefaultCoder(SerializableCoder.class)
    public static class Event implements Serializable {
        String userId;
        String eventId;
        String metadata;
        Instant timestamp;

        public Event(String userId, String eventId, String metadata, Instant timestamp) {
            this.userId = userId;
            this.eventId = eventId;
            this.metadata = metadata;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "Event{userId='" + userId + "', eventId='" + eventId +
                    "', metadata='" + metadata + "', timestamp=" + timestamp + "}";
        }
    }

    public static class PrintResultsFn extends DoFn<KV<String, Event>, Void> {
        @ProcessElement
        public void process(@Element KV<String, Event> kv,
                            BoundedWindow window,
                            PaneInfo pane) {
            IntervalWindow iw = (IntervalWindow) window;
            System.out.println("User=" + kv.getKey() +
                    " | Window=[" + iw.start() + " - " + iw.end() + "]" +
                    " | Pane=" + pane.getIndex() +
                    (pane.isFirst() ? " FIRST" : "") +
                    (pane.isLast() ? " LAST" : ""));
            System.out.println("  " + kv.getValue());
        }
    }

    public static class ExtractUserId implements org.apache.beam.sdk.transforms.SerializableFunction<Event, String> {
        @Override public String apply(Event e) { return e.userId; }
    }

    @Rule public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void test_windows_and_triggers() {
        Instant start = Instant.parse("2025-01-01T00:00:00Z");

        TestStream<Event> events = TestStream.create(SerializableCoder.of(Event.class))
                .advanceWatermarkTo(start)

                // userA hits 3 quickly (EARLY), userB 1 event
                .addElements(
                        TimestampedValue.of(new Event("userA", UUID.randomUUID().toString(), "m1",
                                start.plus(Duration.standardSeconds(5))),  start.plus(Duration.standardSeconds(5))),
                        TimestampedValue.of(new Event("userA", UUID.randomUUID().toString(), "m2",
                                start.plus(Duration.standardSeconds(10))), start.plus(Duration.standardSeconds(10))),
                        TimestampedValue.of(new Event("userA", UUID.randomUUID().toString(), "m3",
                                start.plus(Duration.standardSeconds(15))), start.plus(Duration.standardSeconds(15))),
                        TimestampedValue.of(new Event("userB", UUID.randomUUID().toString(), "m1",
                                start.plus(Duration.standardSeconds(20))), start.plus(Duration.standardSeconds(20))),
                        TimestampedValue.of(new Event("userA", UUID.randomUUID().toString(), "m4",
                                start.plus(Duration.standardSeconds(40))), start.plus(Duration.standardSeconds(40)))
                )
                .advanceWatermarkTo(start.plus(Duration.standardMinutes(1)))
                .advanceWatermarkToInfinity();

        PCollection<Event> input = pipeline.apply(events);

        input
                .apply("KeyByUser", WithKeys.of(new ExtractUserId()))
                .apply("WindowPerUser",
                        Window.<KV<String, Event>>into(FixedWindows.of(Duration.standardMinutes(1)))
                                .triggering(
                                        Repeatedly.forever(
                                                AfterFirst.of(
                                                        AfterPane.elementCountAtLeast(3),
                                                        AfterWatermark.pastEndOfWindow())))
                                .withAllowedLateness(Duration.ZERO)
                                .discardingFiredPanes())
                // 🚨 Removed GroupByKey: it forces recombining everything
                .apply("Print", ParDo.of(new PrintResultsFn()));

        pipeline.run().waitUntilFinish();
    }
}
