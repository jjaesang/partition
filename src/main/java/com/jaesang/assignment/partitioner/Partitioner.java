package com.jaesang.assignment.partitioner;

/**
 * Created by jaesang on 2019-07-22.
 */
public abstract class Partitioner {

    public abstract int getPartition(String key);

    public abstract int getPartitionNum();

}
