package com.linkedin.camus.coders;

public class CamusWrapperLight<R> {
    protected R record;
    protected long timestamp;

    public CamusWrapperLight(R record, long timestamp) {
        this.record = record;
        this.timestamp = timestamp;
    }

    /**
     * Returns the payload record for a single message
     * @return
     */
    public R getRecord() {
        return record;
    }

    /**
     * Returns current if not set by the decoder
     * @return
     */
    public long getTimestamp() {
        return timestamp;
    }
}
