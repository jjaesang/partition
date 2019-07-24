package com.jaesang.assignment.broker;

import com.jaesang.assignment.partitioner.AsciiCodePartitioner;
import com.jaesang.assignment.partitioner.Partitioner;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.concurrent.*;

public class FilePartitioningBroker {

    private static Logger logger = Logger.getLogger(FilePartitioningBroker.class);

    private final ConcurrentMap<Integer, Partition> topic = new ConcurrentHashMap<Integer, Partition>();
    private final BlockingQueue<Integer> assignedPartition = new LinkedBlockingQueue<Integer>();

    private Partitioner partitioner;


    LinkedBlockingQueue<Message> queue;
    HashSet<String> keySets;

    public FilePartitioningBroker(int partitionNum) {
        partitioner = new AsciiCodePartitioner(partitionNum);
       // setUpPartitionInfo();
    }

//    private void setUpPartitionInfo() {
//        int partitionNum = partitioner.getPartitionNum();
//        for (int partitionId = 0; partitionId < partitionNum; partitionId++) {
//            topic.put(partitionId, new Partition(new LinkedBlockingQueue<Message>(), new HashSet<String>()));
//        }
//    }

    public void produceMessage(Message message) {
        try {
            int partitionId = partitioner.getPartition(message.getKey());
            if (topic.containsKey(partitionId)) {
                queue = topic.get(partitionId).getQueue();
                keySets = topic.get(partitionId).getKeySets();
            } else {
                logger.debug(
                        "allocated new key " + partitionId + " so, create new LinkedBlockingQueue");
                queue = new LinkedBlockingQueue<Message>();
                keySets = new HashSet<String>();
            }

            queue.offer(message, 1, TimeUnit.SECONDS);
            keySets.add(message.getKey());

            topic.put(partitionId, new Partition(queue, keySets));

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

//    public void produceMessage(Message message) {
//        String key = message.getKey();
//
//        int partitionId = partitioner.getPartition(key);
//        Partition partition = topic.get(partitionId);
//
//        if (partition.addQueueMessage(message) && partition.addKeySets(message.getKey()))
//            topic.put(partitionId, partition);
//        else {
//            throw new RuntimeException("produce message is failed .. " + message.toString());
//        }
//
//
//    }

    /**
     * 각 Consumer Thread 당, 할당된 Partition의 Queue에서 1개씩 데이터를 가져옴
     */
    public Message consumeMessage(LinkedBlockingQueue<Message> queue) {

        Message message = null;
        try {
            message = queue.poll(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return message;

    }


    /**
     * Consumer Thread 시작 전, Consuming할 파티션 정보를 저장
     */
    public void setAssignPartition() {

        try {
            for (int partitionId : topic.keySet()) {
                assignedPartition.offer(partitionId, 1, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * 각 Consumer Thread 마다, FIFO 방식으로 파티션 할당
     *
     * @return Partition
     */
    public Partition getAssignedPartition() {

        if (assignedPartition.isEmpty()) {
            throw new RuntimeException("does not have allocated partition for consumer ..");
        }

        int partitionId = 0;
        try {
            partitionId = assignedPartition.poll(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out
                .println(Thread.currentThread().getName() + " getAssignPartition : " + partitionId);
        return topic.get(partitionId);

    }


}
