package com.example;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.joda.time.Instant;

import java.io.Serializable;

@DefaultCoder(SerializableCoder.class)
public class Event implements Serializable {
    public String userId;
    public String eventId;
    public String metadata;
    public Instant timestamp;

    public Event() {}

    public Event(String userId, String eventId, String metadata, Instant timestamp) {
        this.userId = userId;
        this.eventId = eventId;
        this.metadata = metadata;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "userId='" + userId + '\'' +
                ", eventId='" + eventId + '\'' +
                ", metadata='" + metadata + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
