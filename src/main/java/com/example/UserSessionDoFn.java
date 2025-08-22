package com.example;

import com.example.Event;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.Optional;

public class UserSessionDoFn extends DoFn<KV<String, Event>, KV<String, Iterable<Event>>> {

    private static final int MAX_EVENTS = 3;
    private static final Duration MAX_DURATION = Duration.standardMinutes(3);

    @StateId("events")
    private final StateSpec<BagState<Event>> eventsSpec = StateSpecs.bag();

    @StateId("count")
    private final StateSpec<ValueState<Integer>> countSpec = StateSpecs.value();

    @StateId("firstEventTime")
    private final StateSpec<ValueState<Instant>> firstEventTimeSpec = StateSpecs.value();

    @StateId("userId")
    private final StateSpec<ValueState<String>> userIdSpec = StateSpecs.value();

    @TimerId("expiryTimer")
    private final TimerSpec expiryTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void processElement(
            ProcessContext c,
            @StateId("events") BagState<Event> events,
            @StateId("count") ValueState<Integer> count,
            @StateId("firstEventTime") ValueState<Instant> firstEventTime,
            @StateId("userId") ValueState<String> userIdState,
            @TimerId("expiryTimer") org.apache.beam.sdk.state.Timer expiryTimer) {

        Event event = c.element().getValue();
        events.add(event);

        Integer currentCount = Optional.ofNullable(count.read()).orElse(0) + 1;
        count.write(currentCount);

        if (userIdState.read() == null) {
            userIdState.write(c.element().getKey());
        }

        Instant sessionStart = firstEventTime.read();
        if (sessionStart == null) {
            sessionStart = event.timestamp;
            firstEventTime.write(sessionStart);
            expiryTimer.set(sessionStart.plus(MAX_DURATION));
        }

        if (currentCount >= MAX_EVENTS) {
            c.output(KV.of(c.element().getKey(), events.read()));
            events.clear();
            count.clear();
            firstEventTime.clear();
            expiryTimer.clear();
            userIdState.clear();
        }
    }

    @OnTimer("expiryTimer")
    public void onExpiry(
            OnTimerContext c,
            @StateId("events") BagState<Event> events,
            @StateId("count") ValueState<Integer> count,
            @StateId("firstEventTime") ValueState<Instant> firstEventTime,
            @StateId("userId") ValueState<String> userIdState) {

        String key = userIdState.read();
        Iterable<Event> bag = events.read();
        boolean hasEvents = bag.iterator().hasNext();

        if (key != null && hasEvents) {
            c.output(KV.of(key, bag));
        }

        events.clear();
        count.clear();
        firstEventTime.clear();
        userIdState.clear();
    }
}