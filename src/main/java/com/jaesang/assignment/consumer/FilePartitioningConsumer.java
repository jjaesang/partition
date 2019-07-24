package com.jaesang.assignment.consumer;

import com.jaesang.assignment.broker.Message;
import com.jaesang.assignment.broker.FilePartitioningBroker;
import com.jaesang.assignment.broker.Partition;

import org.apache.log4j.Logger;


import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class FilePartitioningConsumer implements Runnable {
    private static Logger logger = Logger.getLogger(FilePartitioningConsumer.class);

    private static final String FILE_EXTENSION = ".txt";

    private FilePartitioningBroker filePartitioningBroker;
    private String outputPath;

    public FilePartitioningConsumer(String outputPath, FilePartitioningBroker filePartitioningBroker) {
        this.outputPath = outputPath;
        this.filePartitioningBroker = filePartitioningBroker;
    }

    /**
     * 각 Thread 당 Consuming할 파티션 정보를 받아옴
     * 해당 파티션 내, 다수의 키가 존재할 수 있기 때문에, 해당 파티션에 들어있는 키에 대한 Meta정보를 이용해
     * 작업할 Key에 대한 파일을 생성함
     */
    public void run() {

        Partition partition = filePartitioningBroker.getAssignedPartition();

        ConcurrentHashMap<String, FilePartitioningConsumerWriter> writerMap = new ConcurrentHashMap<>();
        for (String key : partition.getKeySets()) {
            writerMap.put(key, new FilePartitioningConsumerWriter(Paths.get(outputPath, key + FILE_EXTENSION)));
        }

        LinkedBlockingQueue<Message> queue = partition.getQueue();
        Message message;
        while (!queue.isEmpty()) {
            message = filePartitioningBroker.consumeMessage(queue);
            writerMap.get(message.getKey()).write(message);
        }

        /**
         * 해당 파티션의 단어를 모두 읽은 뒤, Writer는 한번에 정리
         */
        for (FilePartitioningConsumerWriter writer : writerMap.values()) {
            logger.debug(writer.getFileName() +" is closed .. ");
            writer.close();
        }

    }


}

