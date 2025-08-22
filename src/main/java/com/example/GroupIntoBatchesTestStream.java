package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

class GroupIntoBatchesTestStream {

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        // Simulate streaming events with TestStream
        TestStream<KV<String, String>> events = TestStream.create(
                        KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
                // t=0
                .addElements(
                        KV.of("userA", "event1"),
                        KV.of("userA", "event2"),
                        KV.of("userA", "event3"),
                        KV.of("userB", "event1"))
                .advanceProcessingTime(Duration.standardSeconds(6)) // advance beyond 5s → flush
                // t=6s
                .addElements(
                        KV.of("userA", "event4"),
                        KV.of("userB", "event2"))
                .advanceProcessingTime(Duration.standardSeconds(6)) // flush again
                .advanceWatermarkToInfinity();

        PCollection<KV<String, Iterable<String>>> batched =
                p.apply(events)
                        .apply("GroupIntoBatches",
                                GroupIntoBatches.<String, String>ofSize(10)
                                        .withMaxBufferingDuration(Duration.standardSeconds(5)));

        // Print results with window details
        batched.apply("PrintBatches", ParDo.of(new PrintBatchesFn()));

        p.run().waitUntilFinish();
    }

    public static class PrintBatchesFn extends DoFn<KV<String, Iterable<String>>, Void> {
        private static final Logger LOG = LoggerFactory.getLogger(PrintBatchesFn.class);
        private static final AtomicLong batchCounter = new AtomicLong(0);

        @ProcessElement
        public void processElement(ProcessContext c) {
            String user = c.element().getKey();
            Iterable<String> events = c.element().getValue();
            long currentBatchId = batchCounter.incrementAndGet();

            List<String> eventList = StreamSupport.stream(events.spliterator(), false)
                    .collect(Collectors.toList());

            StringBuilder sb = new StringBuilder();
            sb.append(String.format("Batch %d (User: %s, Total: %d events) - Events: [",
                    currentBatchId, user, eventList.size()));

            sb.append(String.join(", ", eventList));

            sb.append("] ");

            sb.append("\n");

            LOG.info(sb.toString());
        }
    }
}
