package com.jaesang.assignment.broker;

import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 각 파티션에 하나의 큐와, 파티션에 들어있는 Key값에 대한 메타정보를 관리
 */
public class Partition {
    private LinkedBlockingQueue<Message> queue;
    private HashSet<String> keySets;

    public Partition(LinkedBlockingQueue<Message> queue, HashSet<String> keySets) {
        this.queue = queue;
        this.keySets = keySets;
    }

    public LinkedBlockingQueue<Message> getQueue() {
        return queue;
    }

    public void setQueue(LinkedBlockingQueue<Message> queue) {
        this.queue = queue;
    }

    public  boolean addQueueMessage(Message message){
        return this.queue.offer(message);
    }

    public HashSet<String> getKeySets() {
        return keySets;
    }

    public void setKeySets(HashSet<String> keySets) {
        this.keySets = keySets;
    }

    public boolean addKeySets(String key){
        return this.keySets.add(key);
    }

    public int getQueueSize() {
        return queue.size();
    }
}
