package com.jaesang.assignment.partitioner;

/**
 * Key HashCode 기반 파티셔너
 * 사용하지 않은 파티셔너
 */
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
