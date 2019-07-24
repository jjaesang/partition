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

    private final Set<String> writtenWordset = new HashSet<String>();

    private Path fileName;
    private BufferedWriter writer;

    public FilePartitioningConsumerWriter(Path fileName) {
        this.fileName = fileName;
        try {
            writer = new BufferedWriter(new java.io.FileWriter(fileName.toFile(), true));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

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

    @Override
    public void close() {

        try {
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean isDuplicate(Message message) {
        return writtenWordset.contains(message.getValue());
    }

    private boolean isNotDuplicate(Message message) {
        return !isDuplicate(message);
    }


}
