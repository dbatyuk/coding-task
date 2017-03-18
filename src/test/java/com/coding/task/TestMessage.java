package com.coding.task;

/**
 * Created by dbatyuk on 28.02.2017.
 */
public class TestMessage {

    private final String type;
    private final int count;

    public TestMessage(String type, int count) {
        this.type = type;
        this.count = count;
    }

    public String getType() {
        return type;
    }

    public int getCount() {
        return count;
    }

    @Override
    public String toString() {
        return "TestMessage{" +
                "type='" + type + '\'' +
                ", count=" + count +
                '}';
    }
}
