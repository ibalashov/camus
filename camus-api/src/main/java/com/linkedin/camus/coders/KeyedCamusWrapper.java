package com.linkedin.camus.coders;

public class KeyedCamusWrapper<KEY, MESSAGE> extends CamusWrapperBase<MESSAGE> {

    protected KEY key;

    public KeyedCamusWrapper(KEY key, MESSAGE record, long timestamp) {
        super(record, timestamp);
        this.key = key;
    }

    public KEY getKey() {
        return key;
    }
}
