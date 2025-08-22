package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor; //         Used for TypeDescriptor.of(Class<T>)
import org.apache.beam.sdk.values.TypeDescriptors; //         Used for TypeDescriptors.kvs() and TypeDescriptors.strings()

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Comparator; //         For sorting events within a batch
import java.util.concurrent.atomic.AtomicLong; //         For batch numbering

import org.slf4j.Logger; //         For controlled logging
import org.slf4j.LoggerFactory;


public class ClickstreamProcessor {

    /**
     * Represents a single click event with user ID, timestamp, event type, and arbitrary data.
     * This class must be Serializable for Apache Beam to process it.
     */
    public static class ClickEvent implements Serializable {
        private String userId;
        private long timestamp; //         Event timestamp in milliseconds (epoch)
        private String eventType;
        private String eventData;

        //         Default constructor is required for Beam's serialization/ deserialization
        public ClickEvent() {}

        public ClickEvent(String userId, long timestamp, String eventType, String eventData) {
            this.userId = userId;
            this.timestamp = timestamp;
            this.eventType = eventType;
            this.eventData = eventData;
        }

        //         Getters for Beam to access properties
        public String getUserId() {
            return userId;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public String getEventType() {
            return eventType;
        }

        public String getEventData() {
            return eventData;
        }

        @Override
        public String toString() {
            return "ClickEvent{" +
                    "userId='" + userId + '\'' +
                    ", timestamp=" + new Instant(timestamp) + //         Human-readable timestamp
                    ", eventType='" + eventType + '\'' +
                    ", eventData='" + eventData + '\'' +
                    '}';
        }
    }

    /**
     * A DoFn that simulates hitting an external API with a batch of click events.
     * This DoFn receives a KV pair where the key is the userId and the value is an Iterable of ClickEvents.
     */
    public static class ApiCallerFn extends DoFn<KV<String, Iterable<ClickEvent>>, Void> {

        //         SLF4J Logger for more controlled and readable output
        private static final Logger LOG = LoggerFactory.getLogger(ApiCallerFn.class);

        //         Formatter for consistent date/time output
        private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("HH:mm:ss.SSS");
        //         Use AtomicLong for a simple, thread-safe counter for batches
        private static final AtomicLong batchCounter = new AtomicLong(0);

        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<String, Iterable<ClickEvent>> element = c.element();
            String userId = element.getKey();
            List<ClickEvent> eventList = new ArrayList<>();
            element.getValue().forEach(eventList::add); //         Convert Iterable to List for easier manipulation

            long currentBatchId = batchCounter.incrementAndGet(); //         Get unique ID for this batch
            StringBuilder logBuilder = new StringBuilder(); //         Use StringBuilder for atomic logging

            if (eventList.isEmpty()) {
                logBuilder.append(String.format("--- BATCH %d START (User: %s) ---\n", currentBatchId, userId));
                logBuilder.append("Warning: Received an empty batch. Skipping API call.\n");
                logBuilder.append(String.format("--- BATCH %d END (User: %s) ---\n\n", currentBatchId, userId));
                LOG.info(logBuilder.toString()); //         Print the full message at once using the logger
                return;
            }

            //         Sort events by timestamp for consistency in the API payload and clearer logging
            eventList.sort(Comparator.comparingLong(ClickEvent::getTimestamp));

            //         Extract relevant timestamps and details for logging
            Instant firstEventTime = new Instant(eventList.get(0).getTimestamp());
            Instant lastEventTime = new Instant(eventList.get(eventList.size() - 1).getTimestamp());
            long totalDurationMillis = lastEventTime.getMillis() - firstEventTime.getMillis();
            Duration actualDuration = Duration.millis(totalDurationMillis);

            //         Append detailed batch information to the StringBuilder
            logBuilder.append(String.format("--- BATCH %d START (User: %s) ---\n", currentBatchId, userId));
            logBuilder.append(String.format("  API Call for User: %s\n", userId));
            logBuilder.append(String.format("  Total Events in Batch: %d\n", eventList.size()));
            logBuilder.append(String.format("  Batch Start Time (Event Time): %s\n", FORMATTER.print(firstEventTime)));
            logBuilder.append(String.format("  Batch End Time (Event Time): %s\n", FORMATTER.print(lastEventTime)));
            logBuilder.append(String.format("  Actual Batch Duration: %s\n", actualDuration.toString()));

            //         Append ALL events in the batch for clear demonstration
            logBuilder.append("  Events Accumulated in this Window:\n");
            for (ClickEvent event : eventList) {
                logBuilder.append(String.format("    - [%s] Type: %s, Data: %s\n",
                        FORMATTER.print(new Instant(event.getTimestamp())),
                        event.getEventType(),
                        event.getEventData()));
            }

            logBuilder.append(String.format("--- BATCH %d END (User: %s) ---\n\n", currentBatchId, userId));
            LOG.info(logBuilder.toString()); //         Print the full, constructed message at once using the logger
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline pipeline = Pipeline.create(options);

        //         Define batching parameters
        final Duration groupIntoBatchesMaxDuration = Duration.standardSeconds(10); //         Batch closes after 10 seconds from first event
        final long groupIntoBatchesMaxEvents = 5;                             //         Batch closes after 5 events

        pipeline
                .apply("SimulateClickEvents", Create.of(
                        //         User 1: 1 event/sec, first event at 9:59:00.
                        //         5 events accumulated by 9:59:02.000. Batch should fire at 9:59:02.000.
                        //         Next event at 9:59:03.000 starts a NEW batch.
                        new ClickEvent("user1", 1678886340000L, "click", "home"),   //         9:59:00.000
                        new ClickEvent("user1", 1678886340500L, "view", "productX"), //         9:59:00.500
                        new ClickEvent("user1", 1678886341000L, "add_to_cart", "productX"), //         9:59:01.000
                        new ClickEvent("user1", 1678886341500L, "checkout", "productY"), //         9:59:01.500
                        new ClickEvent("user1", 1678886342000L, "purchase", "productZ"), //         9:59:02.000 (5th event, triggers batch)
                        new ClickEvent("user1", 1678886343000L, "return", "productZ"), //         9:59:03.000 (starts new batch for user1)

                        //         User 2: Only one event at 10:00:00.000. No more events.
                        //         Batch should fire after 10 seconds (at 10:00:10.000) with 1 event.
                        new ClickEvent("user2", 1678886400000L, "search", "shoes"),    //         10:00:00.000

                        //         User 3: 2 events/sec, first event at 10:01:00.000.
                        //         5 events accumulated by 10:01:00.400. Batch should fire at 10:01:00.400.
                        //         Next event at 10:01:00.500 starts a NEW batch.
                        new ClickEvent("user3", 1678886460000L, "click", "ad1"),      //         10:01:00.000
                        new ClickEvent("user3", 1678886460100L, "view", "pageA"),     //         10:01:00.100
                        new ClickEvent("user3", 1678886460200L, "click", "ad2"),     //         10:01:00.200
                        new ClickEvent("user3", 1678886460300L, "view", "pageB"),     //         10:01:00.300
                        new ClickEvent("user3", 1678886460400L, "share", "article"), //         10:01:00.400 (5th event, triggers batch)
                        new ClickEvent("user3", 1678886460500L, "comment", "post1"), //         10:01:00.500 (starts new batch for user3)

                        //         User 4: Demonstrates time-triggered batch closure and new window
                        //         Events 1-4 are within 10 seconds. 5th event arrives AFTER 10 seconds.
                        new ClickEvent("user4", 1678887000000L, "login", "app"),        //         10:10:00.000 (Event 1, Batch 1 begins)
                        new ClickEvent("user4", 1678887002000L, "profile_view", "self"), //         10:10:02.000 (Event 2)
                        new ClickEvent("user4", 1678887004000L, "settings_open", "privacy"),//         10:10:04.000 (Event 3)
                        new ClickEvent("user4", 1678887006000L, "logout", "manual"),     //         10:10:06.000 (Event 4)
                        //         At this point (10:10:06), 4 events are in Batch 1. Max events (5) not hit.
                        //         Duration (6 seconds) is less than maxBufferingDuration (10 seconds).
                        //         Batch 1 will be emitted when 10 seconds pass from first event (10:10:00 + 10s = 10:10:10.000).
                        new ClickEvent("user4", 1678888000000L, "re_login", "new_session") //         10:10:11.000 (Event 5: This event arrives *after* 10:10:10.000.
                        //         So it starts a NEW Batch 2 for user4.)
                ))
                .apply("AssignEventTimestamps", WithTimestamps.of((ClickEvent event) -> new Instant(event.getTimestamp())))
                //         Adjusted to use TypeDescriptors.kvs and TypeDescriptors.strings as per your environment's observation.
                .apply("KeyByUserId", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(ClickEvent.class)))
                        .via((ClickEvent event) -> KV.of(event.getUserId(), event)))
                .apply("GroupIntoBatchesPerUser",
                        GroupIntoBatches.<String, ClickEvent>ofSize(groupIntoBatchesMaxEvents)
                                .withMaxBufferingDuration(groupIntoBatchesMaxDuration)
                )
                .apply("CallApiWithBatches", ParDo.of(new ApiCallerFn()));

        pipeline.run().waitUntilFinish();
    }
}


//        INFO: --- BATCH 3 START (User: user3) ---
//        API Call for User: user3
//        Total Events in Batch: 1
//        Batch Start Time (Event Time): 13:21:00.500
//        Batch End Time (Event Time): 13:21:00.500
//        Actual Batch Duration: PT0S
//        Events Accumulated in this Window:
//                - [13:21:00.500] Type: comment, Data: post1
//        --- BATCH 3 END (User: user3) ---
//
//
//        Aug 21, 2025 1:05:07 PM com.example.ClickstreamProcessor$ApiCallerFn processElement
//        INFO: --- BATCH 6 START (User: user4) ---
//        API Call for User: user4
//        Total Events in Batch: 5
//        Batch Start Time (Event Time): 13:30:00.000
//        Batch End Time (Event Time): 13:30:11.000
//        Actual Batch Duration: PT11S
//        Events Accumulated in this Window:
//                - [13:30:00.000] Type: login, Data: app
//            - [13:30:02.000] Type: profile_view, Data: self
//            - [13:30:04.000] Type: settings_open, Data: privacy
//            - [13:30:06.000] Type: logout, Data: manual
//            - [13:30:11.000] Type: re_login, Data: new_session
//        --- BATCH 6 END (User: user4) ---
//
//
//        Aug 21, 2025 1:05:07 PM com.example.ClickstreamProcessor$ApiCallerFn processElement
//        INFO: --- BATCH 2 START (User: user1) ---
//        API Call for User: user1
//        Total Events in Batch: 5
//        Batch Start Time (Event Time): 13:19:00.000
//        Batch End Time (Event Time): 13:19:02.000
//        Actual Batch Duration: PT2S
//        Events Accumulated in this Window:
//                - [13:19:00.000] Type: click, Data: home
//            - [13:19:00.500] Type: view, Data: productX
//            - [13:19:01.000] Type: add_to_cart, Data: productX
//            - [13:19:01.500] Type: checkout, Data: productY
//            - [13:19:02.000] Type: purchase, Data: productZ
//        --- BATCH 2 END (User: user1) ---
//
//
//        Aug 21, 2025 1:05:07 PM org.apache.beam.sdk.util.MutationDetectors$CodedValueMutationDetector verifyUnmodifiedThrowingCheckedExceptions
//        WARNING: Coder of type class org.apache.beam.sdk.coders.KvCoder has a #structuralValue method which does not return true when the encoding of the elements is equal. Element KV{user3, [ClickEvent{userId='user3', timestamp=2023-03-15T13:21:00.500Z, eventType='comment', eventData='post1'}]}
//        Aug 21, 2025 1:05:07 PM com.example.ClickstreamProcessor$ApiCallerFn processElement
//        INFO: --- BATCH 1 START (User: user3) ---
//        API Call for User: user3
//        Total Events in Batch: 5
//        Batch Start Time (Event Time): 13:21:00.000
//        Batch End Time (Event Time): 13:21:00.400
//        Actual Batch Duration: PT0.400S
//        Events Accumulated in this Window:
//                - [13:21:00.000] Type: click, Data: ad1
//            - [13:21:00.100] Type: view, Data: pageA
//            - [13:21:00.200] Type: click, Data: ad2
//            - [13:21:00.300] Type: view, Data: pageB
//            - [13:21:00.400] Type: share, Data: article
//        --- BATCH 1 END (User: user3) ---
//
//
//        Aug 21, 2025 1:05:07 PM com.example.ClickstreamProcessor$ApiCallerFn processElement
//        INFO: --- BATCH 5 START (User: user2) ---
//        API Call for User: user2
//        Total Events in Batch: 1
//        Batch Start Time (Event Time): 13:20:00.000
//        Batch End Time (Event Time): 13:20:00.000
//        Actual Batch Duration: PT0S
//        Events Accumulated in this Window:
//                - [13:20:00.000] Type: search, Data: shoes
//        --- BATCH 5 END (User: user2) ---
//
//
//        Aug 21, 2025 1:05:07 PM com.example.ClickstreamProcessor$ApiCallerFn processElement
//        INFO: --- BATCH 4 START (User: user1) ---
//        API Call for User: user1
//        Total Events in Batch: 1
//        Batch Start Time (Event Time): 13:19:03.000
//        Batch End Time (Event Time): 13:19:03.000
//        Actual Batch Duration: PT0S
//        Events Accumulated in this Window:
//                - [13:19:03.000] Type: return, Data: productZ
//        --- BATCH 4 END (User: user1) ---
