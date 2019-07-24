package com.jaesang.assignment.consumer;

import com.jaesang.assignment.broker.Message;

public abstract class FilePartitioningWriter {

    public abstract void write(Message message);

    public abstract void close();
}
