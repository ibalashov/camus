package com.linkedin.camus.coders;

public class CamusWrapperBase<MESSAGE> {
    protected MESSAGE record;
    protected long timestamp;

    public CamusWrapperBase(MESSAGE record, long timestamp) {
        this.record = record;
        this.timestamp = timestamp;
    }

    /**
     * Returns the payload record for a single message
     * @return
     */
    public MESSAGE getRecord() {
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
