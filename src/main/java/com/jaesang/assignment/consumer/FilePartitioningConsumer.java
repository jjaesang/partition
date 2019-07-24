package com.jaesang.assignment.consumer;

import com.jaesang.assignment.broker.Message;
import com.jaesang.assignment.broker.FilePartitioningBroker;
import com.jaesang.assignment.broker.Partition;

import org.apache.log4j.Logger;


import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 　　1) 파티션에서 순차적으로 단어를 1개씩 가져온다.
 * 　　　　　2) 단어가 알파벳으로 시작한다면 단어의 첫 알파벳에 해당하는 파일 끝에 주어진 단어를 추가 해야한다.
 * 　　　　　　 예) apple, Apple은 a.txt 파일 끝에 추가 해야한다. (대소문자 구분없음)
 * 　　　　　3) 단어가 숫자로 시작한다면 number.txt 파일 끝에 주어진 단어를 추가 해야한다.
 * 　　　　　　 예) 1-point, 2-point는 number.txt 파일 끝에 추가 해야한다.
 * 　　　　　4) 주어진 단어가 대상 파일에 이미 쓰여진 단어인지 대소문자 구분 없이 중복검사를 수행하고, 중복되지 않은 단어라면 대상 파일 끝에 단어를 추가한다.
 */
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

        for (FilePartitioningConsumerWriter writer : writerMap.values()) {
            writer.close();
        }

    }


}

