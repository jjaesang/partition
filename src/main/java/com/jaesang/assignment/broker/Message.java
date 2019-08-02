package com.jaesang.assignment.broker;

/**
 * 단어에 대한 키와 함께 Producer/Consumer의 레코드 관리
 */
public final class Message {

    private final String key;
    private final String value;

    public Message(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return this.key;
    }

    public String getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        return "Message [ key = " + key + ", value = " + value + " ]";
    }
}
