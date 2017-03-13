package com.coding.task;

import com.beust.jcommander.internal.Lists;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.*;

/**
 * Created by dbatyuk on 28.02.2017.
 */
public class CombinerImplTest {

    private static final int DEFAULT_CAPACITY = 10;

    private Producer producerA;
    private Producer producerB;
    private Consumer consumer;

    @BeforeMethod
    public void beforeMethod() {
        BlockingQueue<Message> inputQueueA = new ArrayBlockingQueue<>(DEFAULT_CAPACITY);
        BlockingQueue<Message> inputQueueB = new LinkedBlockingQueue<>(DEFAULT_CAPACITY);
        this.producerA = new Producer("A", inputQueueA, 10, 10);
        this.producerB = new Producer("B", inputQueueB, 100, 10);

        SynchronousQueue<Message> outputQueue = new SynchronousQueue<>();
        this.consumer = new Consumer(outputQueue);
    }

    @Test
    public void test() {
        try {
            Combiner<Message> messageCombiner = CombinerImpl.createStarted(consumer.getOutputQueue());

            messageCombiner.addInputQueue(producerA.getInputQueue(), 9.5, 100, TimeUnit.MILLISECONDS);
            messageCombiner.addInputQueue(producerB.getInputQueue(), 0.5, 1, TimeUnit.MINUTES);

            new Thread(consumer).start();

            ExecutorService executorService = Executors.newFixedThreadPool(2);
            executorService.invokeAll(Lists.newArrayList(producerA, producerB));
        } catch (Combiner.CombinerException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private class Producer implements Callable<Void> {

        private final String type;
        private final BlockingQueue<Message> inputQueue;
        private final int numbOfElements;
        private final long sleep;

        private Producer(String type, BlockingQueue<Message> inputQueue, int numbOfElements, long sleep) {
            this.type = type;
            this.inputQueue = inputQueue;
            this.numbOfElements = numbOfElements;
            this.sleep = sleep;
        }

        public String getType() {
            return type;
        }

        public BlockingQueue<Message> getInputQueue() {
            return inputQueue;
        }

        @Override
        public Void call() {
            for (int i = 0; i < numbOfElements; i++) {
                try {
                    if (sleep != 0) {
                        Thread.sleep(sleep);
                    }
                    this.inputQueue.put(new Message(type, i));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }

    private class Consumer implements Runnable {

        private final SynchronousQueue<Message> outputQueue;

        private Consumer(SynchronousQueue<Message> outputQueue) {
            this.outputQueue = outputQueue;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Message message = this.outputQueue.take();
                    System.out.println(message);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        public SynchronousQueue<Message> getOutputQueue() {
            return outputQueue;
        }
    }

}