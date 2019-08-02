package com.jaesang.assignment;

import com.jaesang.assignment.broker.FilePartitioningBroker;
import com.jaesang.assignment.consumer.FilePartitioningConsumer;
import com.jaesang.assignment.producer.FilePartitioningProducer;
import com.jaesang.assignment.utils.ValidatorUtil;
import org.apache.log4j.Logger;

import java.util.concurrent.*;

public class FilePartitioningJob {
    private static Logger logger = Logger.getLogger(FilePartitioningJob.class);

    private String inputPath;
    private String outputPath;
    private int numPartition;

    private ExecutorService producerExecutor;
    private ExecutorService consumerExecutor;

    private FilePartitioningBroker filePartitioningBroker;

    /**
     * Argument Validation 및 Parsing
     * Producer / Consumer의 ThreadPool 설정
     *
     * @param args
     */
    private void init(String[] args) {

        ValidatorUtil validatorUtil = new ValidatorUtil();
        if (validatorUtil.isNotValidateArgument(args))
            throw new IllegalArgumentException("failed to parse options... ");

        this.inputPath = args[0];
        this.outputPath = args[1];
        this.numPartition = Integer.parseInt(args[2]);

        this.filePartitioningBroker = new FilePartitioningBroker(numPartition);
        this.producerExecutor = Executors.newSingleThreadExecutor();
        this.consumerExecutor = Executors.newFixedThreadPool(numPartition);
    }


    /**
     * Producing하기 전, 브로커에게 Producing할 파티션 정보 설정 요청
     * 설정 된 후, 파일을 읽어 브로커에게 Message 전송
     */
    private void startProducer() {
        logger.info("start producer");

        filePartitioningBroker.setPartitionInfo();
        producerExecutor.execute(new FilePartitioningProducer(inputPath, filePartitioningBroker));

    }

    /**
     * 작업 완료 후, 리소스 정리
     */
    private void stopProducer() {
        logger.info("end producer");

        try {
            producerExecutor.shutdown();
            producerExecutor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            logger.info("interrupted exception occurred");
        }

    }

    /**
     * Consuming하기 전, 브로커에게 각 Consumer Thread가 할당할 파티션 정보 설정 요청
     * 설정 된 후, 브로커에게 할당받은 파티션 정보를 읽어 파일에 저장
     */
    private void startConsumer() {
        logger.info("start consumer");

        filePartitioningBroker.setAssignPartition();
        for (int i = 0; i < numPartition; i++)
            consumerExecutor.execute(new FilePartitioningConsumer(outputPath, filePartitioningBroker));

    }

    /**
     * 작업 완료 후, 리소스 정리
     */
    private void stopConsumer() {
        logger.info("stop consumer");

        try {
            consumerExecutor.shutdown();
            consumerExecutor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.info("interrupted exception occurred");
        }
    }

    public void run() {

        startProducer();
        startConsumer();

        stopProducer();
        stopConsumer();

    }

    private void printUsage() {
        logger.error("\n FilePartitioningJob Usage: [inputFilePath] [outputFilePath] [numberOfPartitions] \n" +
                "\t-[inputFilePath]      Input File Path \n" +
                "\t-[outputFilePath]     MUST be existed Directory\n" +
                "\t-[numberOfPartitions] MUST BE 1 < N < 28) \n\n");
    }

    public static void main(String[] args) {

        FilePartitioningJob filePartitioningJob = new FilePartitioningJob();
        try {
            filePartitioningJob.init(args);
        } catch (IllegalArgumentException e) {
            filePartitioningJob.printUsage();
            return;
        }
        filePartitioningJob.run();

    }
}