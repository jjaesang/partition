package com.jaesang.assignment.broker;

/**
 * 단어에 대한 키와 함께 Producer/Consumer의 레코드 관리
 */
public class Message {

    private String key;
    private String value;

    public Message(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Message [ key = " + key + ", value = " + value+" ]";
    }
}
