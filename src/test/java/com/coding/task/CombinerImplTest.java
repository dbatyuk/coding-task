package com.coding.task;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.concurrent.*;

/**
 * Created by dbatyuk on 28.02.2017.
 */
public class CombinerImplTest {

    private static final int DEFAULT_CAPACITY = 10;

    private Combiner<Message> messageCombiner;
    private Producer producerA;
    private Producer producerB;
    private Consumer consumer;

    @BeforeMethod
    public void beforeMethod() {
        BlockingQueue<Message> inputQueueA = new ArrayBlockingQueue<>(DEFAULT_CAPACITY);
        BlockingQueue<Message> inputQueueB = new LinkedBlockingQueue<>(DEFAULT_CAPACITY);
        this.producerA = new Producer("A", inputQueueA);
        this.producerB = new Producer("B", inputQueueB);

        SynchronousQueue<Message> outputQueue = new SynchronousQueue<>();
        this.consumer = new Consumer(outputQueue);

        this.messageCombiner = CombinerImpl.createAndStart(outputQueue);
    }

    @Test
    public void test(){
        try {
            this.messageCombiner.addInputQueue(producerA.getInputQueue(), 9.5, 10, TimeUnit.SECONDS);
            this.messageCombiner.addInputQueue(producerB.getInputQueue(), 0.5, 10, TimeUnit.SECONDS);

            new Thread(consumer).start();

            new Thread(producerA).start();
            new Thread(producerB).start();
        } catch (Combiner.CombinerException e) {
            e.printStackTrace();
        }
    }

    private class Producer implements Runnable {

        private final String type;
        private final BlockingQueue<Message> inputQueue;

        private Producer(String type, BlockingQueue<Message> inputQueue) {
            this.type = type;
            this.inputQueue = inputQueue;
        }

        public String getType() {
            return type;
        }

        public BlockingQueue<Message> getInputQueue() {
            return inputQueue;
        }

        @Override
        public void run() {
            for (int i =0; i < 10; i++){
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextLong(1500));
                    inputQueue.offer(new Message(type, i));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class Consumer implements Runnable {

        private final SynchronousQueue<Message> outputQueue;

        private Consumer(SynchronousQueue<Message> outputQueue) {
            this.outputQueue = outputQueue;
        }

        @Override
        public void run() {
            while (true){
                try {
                    Message message = this.outputQueue.poll(1, TimeUnit.MINUTES);

                    if (message == null){
                        return;
                    }

                    System.out.println(message);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}