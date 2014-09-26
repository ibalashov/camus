package com.linkedin.camus.etl.kafka.coders;

import com.google.common.base.Charsets;
import com.linkedin.camus.coders.KeyedCamusWrapper;
import kafka.message.Message;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;

public class LatestSchemaKafkaAvroMessageDecoder extends KafkaAvroMessageDecoder {

    @Override
    public KeyedCamusWrapper<String, Record> decode(byte[] key, byte[] message) {
        try {
            GenericDatumReader<Record> reader = new GenericDatumReader<Record>();

            Schema schema = super.registry.getLatestSchemaByTopic(super.topicName).getSchema();

            reader.setSchema(schema);

            Record record = reader.read(
                    null,
                    decoderFactory.jsonDecoder(
                            schema,
                            new String(
                                    message,
                                    //Message.payloadOffset(message.magic()),
                                    Message.MagicOffset(),
                                    message.length - Message.MagicOffset()
                            )
                    )
            );
            String keyStr = new String(key, Charsets.UTF_8);
            return new KeyedCamusWrapper<>(keyStr, record, -1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}