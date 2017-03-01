package com.coding.task;

/**
 * Created by dbatyuk on 28.02.2017.
 */
public class Message {

    private final String type;
    private final int count;

    public Message(String type, int count) {
        this.type = type;
        this.count = count;
    }

    @Override
    public String toString() {
        return "Message{" +
                "type='" + type + '\'' +
                ", count=" + count +
                '}';
    }
}
