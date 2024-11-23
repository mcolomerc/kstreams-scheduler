package org.example;

import java.util.Objects;

public class CustomRecord  {

    private String id;
    private String value;
    private long scheduledTimestamp;

    // No-argument constructor
    public CustomRecord() {}

    public CustomRecord(String id, String value, long timestamp) {
        this.id = id;
        this.value = value;
        this.scheduledTimestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getScheduledTimestamp() {
        return scheduledTimestamp;
    }

    public void setScheduledTimestamp(long scheduledTimestamp) {
        this.scheduledTimestamp = scheduledTimestamp;
    }

    @Override
    public String toString() {
        return "CustomRecord{" +
                "id='" + id + '\'' +
                ", value='" + value + '\'' +
                ", scheduledTimestamp=" + scheduledTimestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        CustomRecord that = (CustomRecord) o;
        return scheduledTimestamp == that.scheduledTimestamp
                && Objects.equals(id, that.id)
                && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, value, scheduledTimestamp);
    }
}
