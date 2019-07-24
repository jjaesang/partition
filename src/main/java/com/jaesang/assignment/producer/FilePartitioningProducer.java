package com.jaesang.assignment.producer;

import com.jaesang.assignment.broker.Message;
import com.jaesang.assignment.broker.FilePartitioningBroker;
import com.jaesang.assignment.utils.KeyGeneratorUtil;
import com.jaesang.assignment.utils.ValidatorUtil;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * 1. 파일을 읽어 각 단어에 대한 유효성 검사
 * 2. 유요한 단어에 대한 Key값 생성 후, Key/Value 형태의 Message로 Broker에게 Produce
 */
public class FilePartitioningProducer implements Runnable {

    private static Logger logger = Logger.getLogger(FilePartitioningProducer.class);

    private Path inputPath;

    private FilePartitioningBroker filePartitioningBroker;

    private ValidatorUtil validatorUtil = new ValidatorUtil();
    private KeyGeneratorUtil keyGeneratorUtil;

    public FilePartitioningProducer(String inputPath,
                                    FilePartitioningBroker filePartitioningBroker) {
        this.inputPath = Paths.get(inputPath);
        this.filePartitioningBroker = filePartitioningBroker;

        keyGeneratorUtil = new KeyGeneratorUtil();
    }

    /**
     * 파일을 읽어, 각 단어에 대한 유효성 검사를 진행함
     * 유효한 단어일 경우, 각 단어에 key값을 생성함
     * key값과 단어의 정보를 Broker에게 전달
     */
    public void run() {
       try (BufferedReader bufferedReader = new BufferedReader(new FileReader(inputPath.toFile()))) {
            while (true) {
                String word = bufferedReader.readLine();
                if (word == null) {
                    break;
                }
                if (validatorUtil.isNotValidWord(word)) {
                     logger.debug("invalid word : " + word);
                    continue;
                }
                String key = keyGeneratorUtil.generateKey(word);
                filePartitioningBroker.produceMessage(new Message(key, word));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
