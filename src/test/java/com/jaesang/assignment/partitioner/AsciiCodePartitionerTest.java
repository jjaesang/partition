package com.jaesang.assignment.partitioner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class AsciiCodePartitionerTest {

    private static List<String> keyList = new ArrayList<String>(
            Arrays.asList("a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z","number"));

    @Test
    public void getPartitionIdFor2Partition() {

        Set<Integer> partitionIdSet = new HashSet<Integer>();
        int numPartition = 2;
        AsciiCodePartitioner asciiCodePartitioner = new AsciiCodePartitioner(numPartition);

        for(String key : keyList)
            partitionIdSet.add(asciiCodePartitioner.getPartition(key));

        assertEquals(numPartition,partitionIdSet.size());
    }

    @Test
    public void getPartitionIdFor5Partition() {

        Set<Integer> partitionIdSet = new HashSet<Integer>();
        int numPartition = 5;
        AsciiCodePartitioner asciiCodePartitioner = new AsciiCodePartitioner(numPartition);

        for(String key : keyList)
            partitionIdSet.add(asciiCodePartitioner.getPartition(key));

        assertEquals(numPartition,partitionIdSet.size());
    }

    @Test
    public void getPartitionIdFor10Partition() {

        Set<Integer> partitionIdSet = new HashSet<Integer>();
        int numPartition = 10;
        AsciiCodePartitioner asciiCodePartitioner = new AsciiCodePartitioner(numPartition);

        for(String key : keyList)
            partitionIdSet.add(asciiCodePartitioner.getPartition(key));

        assertEquals(numPartition,partitionIdSet.size());
    }

    @Test
    public void getPartitionIdFor15Partition() {

        Set<Integer> partitionIdSet = new HashSet<Integer>();
        int numPartition = 15;
        AsciiCodePartitioner asciiCodePartitioner = new AsciiCodePartitioner(numPartition);

        for(String key : keyList)
            partitionIdSet.add(asciiCodePartitioner.getPartition(key));

        assertEquals(numPartition,partitionIdSet.size());
    }

    @Test
    public void getPartitionIdFor27Partition() {

        Set<Integer> partitionIdSet = new HashSet<Integer>();
        int numPartition = 27;
        AsciiCodePartitioner asciiCodePartitioner = new AsciiCodePartitioner(numPartition);

        for(String key : keyList)
            partitionIdSet.add(asciiCodePartitioner.getPartition(key));

        assertEquals(numPartition,partitionIdSet.size());
    }

}