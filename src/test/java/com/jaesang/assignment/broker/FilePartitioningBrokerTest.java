package com.jaesang.assignment.broker;

import com.jaesang.assignment.partitioner.AsciiCodePartitioner;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.HashSet;
import java.util.concurrent.*;

import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class FilePartitioningBrokerTest {

    private AsciiCodePartitioner partitioner;

    private final ConcurrentMap<Integer, Partition> topic = new ConcurrentHashMap<>();

    @Before
    public void setUp() throws Exception {
        int partitionNum = 3;
        partitioner = new AsciiCodePartitioner(partitionNum);

        for (int partitionId = 0; partitionId < partitionNum; partitionId++) {
            topic.put(partitionId, new Partition(new LinkedBlockingQueue<>(), new HashSet<>()));
        }
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void test1setPartitionInfo() {
        int partitionNum = partitioner.getPartitionNum();

        assertEquals(3, partitionNum);
        assertEquals(3, topic.size());

    }

    @Test
    public void test2produceMessage() {

        Message message = new Message("a","abcChoco");
        int partitionId = partitioner.getPartition(message.getKey());
        Partition partition = topic.get(partitionId);

        assertEquals(0, partition.getQueueSize());
        assertEquals(0, partition.getKeySets().size());

        //update & produce NEW Message
        partition.addQueueMessage(message);
        partition.addKeySets(message.getKey());
        topic.put(partitionId, partition);

        Partition updatedPartition = topic.get(partitionId);
        assertEquals(1, updatedPartition.getQueue().size());
        assertEquals(1, updatedPartition.getKeySets().size());

    }

    @Test
    public void test3consumeMessage() throws InterruptedException {

        Message message = new Message("a","abcChoco");
        int partitionId = partitioner.getPartition(message.getKey());
        Partition partition = topic.get(partitionId);
        partition.addQueueMessage(message);
        partition.addKeySets(message.getKey());
        topic.put(partitionId, partition);


        int consumerPartitionId = partitioner.getPartition(message.getKey());
        Partition consumePartition = topic.get(consumerPartitionId);

        Message consumeMessage = consumePartition.getQueue().poll(1, TimeUnit.SECONDS);
        assertEquals("a",  consumeMessage.getKey());
        assertEquals("abcChoco",  consumeMessage.getValue());

    }


}