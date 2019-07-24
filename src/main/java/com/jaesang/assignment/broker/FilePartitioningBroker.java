package com.jaesang.assignment.broker;

import com.jaesang.assignment.partitioner.AsciiCodePartitioner;
import com.jaesang.assignment.partitioner.Partitioner;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.concurrent.*;

/**
 * - Producer로 받은 Message에 대한 파티셔닝 작업 진행
 * - 파티셔닝 정보에 따른, 각 Message을 파티션에 저장
 *
 * - 각 Consumer에게 파티션 정보를 할당
 * - 할당받은 Consumer가 consumer 요청 시, 해당 파티션의 Message을 전송
 */
public class FilePartitioningBroker {

    private static Logger logger = Logger.getLogger(FilePartitioningBroker.class);

    private final ConcurrentMap<Integer, Partition> topic = new ConcurrentHashMap<>();
    private final BlockingQueue<Integer> assignedPartition = new LinkedBlockingQueue<>();

    private Partitioner partitioner;

    public FilePartitioningBroker(int partitionNum) {
        partitioner = new AsciiCodePartitioner(partitionNum);
    }

    /**
     * 저장할 Partition 정보를 사전에 정의
     */
    public void setPartitionInfo() {

        logger.debug("init partition information Before producing Job");
        int partitionNum = partitioner.getPartitionNum();
        for (int partitionId = 0; partitionId < partitionNum; partitionId++) {
            topic.put(partitionId, new Partition(new LinkedBlockingQueue<>(), new HashSet<>()));
        }
    }

    /**
     * Producer가 전달한 Message Key기반 Partition 할당
     * 할당받은 Partition에 데이터 저장 및 Message Key 메타정보 업데이트
     *
     * @param message
     */
    public void produceMessage(Message message) {
        int partitionId = partitioner.getPartition(message.getKey());
        Partition partition = topic.get(partitionId);

        partition.addQueueMessage(message);
        partition.addKeySets(message.getKey());

        topic.put(partitionId, partition);
    }


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

        logger.debug("init assigend partition information for consuming Job");
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
        System.out.println(Thread.currentThread().getName() + " getAssignPartition : " + partitionId);
        return topic.get(partitionId);

    }


}
