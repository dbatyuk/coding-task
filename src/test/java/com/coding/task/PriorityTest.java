package com.coding.task;

import com.coding.task.Combiner.CombinerException;
import com.google.common.collect.Lists;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

/**
 * Created by dbatyuk on 28.02.2017.
 */
public class PriorityTest {

    private static final int DEFAULT_CAPACITY = 100;
    private static final String TYPE_A = "A";
    private static final String TYPE_B = "B";

    private Producer producerA;
    private Producer producerB;
    private Consumer consumer;
    private Combiner<TestMessage> messageCombiner;

    private static void assertPriorities(List<TestMessage> messages, int fromIndex, int toIndex, int typeASize, int typeBSize) {
        List<TestMessage> sublist = messages.subList(fromIndex, toIndex);
        List<TestMessage> typeAMessages = sublist.stream().filter(m -> m.getType().equals(TYPE_A)).collect(Collectors.toList());
        List<TestMessage> typeBMessages = sublist.stream().filter(m -> m.getType().equals(TYPE_B)).collect(Collectors.toList());

        assertEquals(typeAMessages.size(), typeASize);
        assertEquals(typeBMessages.size(), typeBSize);
    }

    @BeforeMethod
    public void beforeMethod() {
        BlockingQueue<TestMessage> inputQueueA = new ArrayBlockingQueue<>(DEFAULT_CAPACITY);
        BlockingQueue<TestMessage> inputQueueB = new LinkedBlockingQueue<>(DEFAULT_CAPACITY);
        this.producerA = new Producer(TYPE_A, inputQueueA, 100);
        this.producerB = new Producer(TYPE_B, inputQueueB, 100);

        SynchronousQueue<TestMessage> outputQueue = new SynchronousQueue<>();
        this.consumer = new Consumer(outputQueue);

        this.messageCombiner = CombinerImpl.createStarted(consumer.getOutputQueue());
    }

    @AfterMethod
    public void afterMethod() {
        CombinerImpl.stop(this.messageCombiner);
    }

    @Test
    public void testPriorityWorks() throws CombinerException, InterruptedException, ExecutionException {
        this.messageCombiner.addInputQueue(this.producerA.getInputQueue(), 9.5, 100, TimeUnit.MILLISECONDS);
        this.messageCombiner.addInputQueue(this.producerB.getInputQueue(), 0.5, 200, TimeUnit.MILLISECONDS);

        ExecutorService executorService = Executors.newFixedThreadPool(3);
        Future<List<TestMessage>> messagesFuture = executorService.submit(this.consumer);

        executorService.invokeAll(Lists.newArrayList(this.producerA, this.producerB));

        List<TestMessage> messages = messagesFuture.get();

        assertPriorities(messages, 0, 100, 95, 5);
        assertPriorities(messages, 100, 200, 5, 95);

        Thread.sleep(100);

        assertFalse(this.messageCombiner.hasInputQueue(this.producerA.getInputQueue()));
        assertFalse(this.messageCombiner.hasInputQueue(this.producerB.getInputQueue()));
    }

    private class Producer implements Callable<Void> {

        private final String type;
        private final BlockingQueue<TestMessage> inputQueue;
        private final int numbOfElements;

        private Producer(String type, BlockingQueue<TestMessage> inputQueue, int numbOfElements) {
            this.type = type;
            this.inputQueue = inputQueue;
            this.numbOfElements = numbOfElements;
        }

        @Override
        public Void call() {
            for (int i = 0; i < this.numbOfElements; i++) {
                try {
                    this.inputQueue.put(new TestMessage(this.type, i));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }

        public BlockingQueue<TestMessage> getInputQueue() {
            return this.inputQueue;
        }
    }

    private class Consumer implements Callable<List<TestMessage>> {

        private final SynchronousQueue<TestMessage> outputQueue;

        private Consumer(SynchronousQueue<TestMessage> outputQueue) {
            this.outputQueue = outputQueue;
        }

        @Override
        public List<TestMessage> call() {
            List<TestMessage> messages = new ArrayList<>();
            while (true) {
                try {
                    TestMessage message = this.outputQueue.poll(1, TimeUnit.SECONDS);

                    if (message == null) {
                        return messages;
                    }

                    messages.add(message);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        public SynchronousQueue<TestMessage> getOutputQueue() {
            return this.outputQueue;
        }
    }

}