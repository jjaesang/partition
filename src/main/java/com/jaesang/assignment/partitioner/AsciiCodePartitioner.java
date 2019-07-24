package com.jaesang.assignment.partitioner;

public class AsciiCodePartitioner extends Partitioner {

    private static String NUMBER = "number";
    private static char NUMBER_MAGIC_CHAR = '{';

    private int partitionNum;

    public AsciiCodePartitioner(int partitionNum) {
        this.partitionNum = partitionNum;
    }

    @Override
    public int getPartition(String key) {
        if (key == null)
            throw new RuntimeException("partition key is null");

        return getAsciiCode(key) % partitionNum;
    }

    @Override
    public int getPartitionNum() {
        return this.partitionNum;

    }


    private int getAsciiCode(String key) {
        if (key.equals(NUMBER))
            return (int) NUMBER_MAGIC_CHAR;

        return (int) key.charAt(0);
    }
}
