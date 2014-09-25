package com.linkedin.camus.coders;

public abstract class KeyedMessageDecoder<K_IN, K_OUT, M_IN, M_OUT> extends MessageDecoder<M_OUT, M_IN> {
    public abstract KeyedCamusWrapper<K_OUT, M_OUT> decode(K_IN key, M_IN message);
}
