package com.linkedin.camus.coders;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

/**
 * Container for messages.  Enables the use of a custom message decoder with knowledge
 * of where these values are stored in the message schema
 *
 * @author kgoodhop
 *
 * @param <MESSAGE> The type of decoded payload
 */
public class CamusWrapper<MESSAGE> extends CamusWrapperBase<MESSAGE> {
    private MapWritable partitionMap;

    public CamusWrapper(MESSAGE record) {
        this(record, System.currentTimeMillis());
    }

    public CamusWrapper(MESSAGE record, long timestamp) {
        this(record, timestamp, "unknown_server", "unknown_service");
    }

    public CamusWrapper(MESSAGE record, long timestamp, String server, String service) {
        super(record, timestamp);
//        this.partitionMap = new MapWritable();
//        partitionMap.put(new Text("server"), new Text(server));
//        partitionMap.put(new Text("service"), new Text(service));
    }

    /**
     * Add a value for partitions
     */
    public void put(Writable key, Writable value) {
        throw new RuntimeException("not implemented");
//        partitionMap.put(key, value);
    }

    /**
     * Get a value for partitions
     * @return the value for the given key
     */
    public Writable get(Writable key) {
        throw new RuntimeException("not implemented");
//        return partitionMap.get(key);
    }

    /**
     * Get all the partition key/partitionMap
     */
    public MapWritable getPartitionMap() {
        throw new RuntimeException("not implemented");
//        return partitionMap;
    }

}
