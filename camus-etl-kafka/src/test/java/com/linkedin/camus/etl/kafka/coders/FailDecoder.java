package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.KeyedCamusWrapper;
import com.linkedin.camus.coders.KeyedMessageDecoder;

public class FailDecoder extends KeyedMessageDecoder<byte[], String, byte[], String> {

    @Override
    public KeyedCamusWrapper<String, String> decode(byte[] key, byte[] message) {
        throw new RuntimeException("decoder failure");
    }

    @Override
    public CamusWrapper<byte[]> decode(String message) {
        throw new RuntimeException("decoder failure");
    }
}
