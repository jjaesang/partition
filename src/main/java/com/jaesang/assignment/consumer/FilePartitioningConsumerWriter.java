package com.jaesang.assignment.consumer;

import com.jaesang.assignment.broker.Message;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

public class FilePartitioningConsumerWriter extends FilePartitioningWriter {
    private static Logger logger = Logger.getLogger(FilePartitioningConsumerWriter.class);

    private final Set<String> writtenWordset = new HashSet<>();

    private Path fileName;
    private BufferedWriter writer;

    /**
     * 각 키 별로 하나의 Writer객체를 생성
     * @param fileName a.txt
     */
    public FilePartitioningConsumerWriter(Path fileName) {
        this.fileName = fileName;
        try {
            writer = new BufferedWriter(new java.io.FileWriter(fileName.toFile(), true));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 해당 단어가 이미 쓰여진 단어를 확인한 후, 쓰여져 있지 않다면 파일에 기록
     * @param message Message("a","Acava")
     */
    @Override
    public void write(Message message) {
        if (isDuplicate(message)) {
            logger.debug("duplicate value in " + fileName + "\t" + message.toString());
            return;
        }
        try {
            String word = message.getValue();
            writer.write(word);
            writer.newLine();
            writtenWordset.add(word);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 파일 쓰기 작업 종류 및 리소스 정리
     */
    @Override
    public void close() {

        try {
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getFileName(){
        return fileName.getFileName().toString();
    }

    /**
     * 한번 쓰여진 단어인지 확인하는 메소드
     * @param message Message("a","Acava")
     * @return true
     */
    private boolean isDuplicate(Message message) {
        return writtenWordset.contains(message.getValue());
    }

    private boolean isNotDuplicate(Message message) {
        return !isDuplicate(message);
    }


}
