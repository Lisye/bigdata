package com.leo.kafka.procuder;

import kafka.producer.Partitioner;

public class OldCustomPartitioner implements Partitioner {

    public OldCustomPartitioner() {
        super();
    }

    public int partition(Object key, int numPartitions) {
        return 0;
    }
}
