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

    private void startProducer() {

        Future producerStatus = producerExecutor.submit(new FilePartitioningProducer(inputPath, filePartitioningBroker));
        try {
            producerStatus.get();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void stopProducer() {

        try {
            producerExecutor.shutdown();
            producerExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.info("interrupted exception occurred");
        }

    }

    private void startConsumer() {

        filePartitioningBroker.setAssignPartition();
        for (int i = 0; i < numPartition; i++)
            consumerExecutor.execute(new FilePartitioningConsumer(outputPath, filePartitioningBroker));

    }

    private void stopConsumer() {
        try {
            consumerExecutor.shutdown();
            consumerExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.info("interrupted exception occurred");
        }
    }

    public void run() {

        logger.info("start producer");
        startProducer();
        logger.info("start consumer");
        startConsumer();
        logger.info("end producer");
        stopProducer();
        logger.info("stop consumer");
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
            //print usage and terminate program
            filePartitioningJob.printUsage();
            return;
        }
        filePartitioningJob.run();

    }
}