package com.jaesang.assignment.partitioner;


public class HashPartitioner extends Partitioner {

    private int numPartition;

    public HashPartitioner(int numPartition) {
        this.numPartition = numPartition;
    }

    public int getPartition(String key) {
        if (key == null)
            throw new RuntimeException("partition key is null");

        return toPositive(key.hashCode()) % numPartition;
    }

    public int getPartitionNum() {
        return this.numPartition;
    }


    private int toPositive(int number) {
        return number & 0x7fffffff;
    }
}
