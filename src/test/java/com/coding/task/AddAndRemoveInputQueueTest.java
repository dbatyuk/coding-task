package com.coding.task;

import com.coding.task.Combiner.CombinerException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;

/**
 * Created by dbatyuk on 18.03.2017.
 */
public class AddAndRemoveInputQueueTest {

    private BlockingQueue<TestMessage> inputQueue;

    private Combiner<TestMessage> messageCombiner;

    @BeforeMethod
    public void beforeMethod() {
        SynchronousQueue<TestMessage> outputQueue = new SynchronousQueue<>();

        this.inputQueue = new LinkedBlockingQueue<>();

        this.messageCombiner = CombinerImpl.createStarted(outputQueue);
    }

    @AfterMethod
    public void afterMethod() {
        CombinerImpl.stop(this.messageCombiner);
    }

    @Test
    public void testAddAndRemove() throws CombinerException {
        this.messageCombiner.addInputQueue(this.inputQueue, 10d, 10, TimeUnit.SECONDS);

        assertTrue(this.messageCombiner.hasInputQueue(this.inputQueue));

        this.messageCombiner.removeInputQueue(this.inputQueue);

        assertFalse(this.messageCombiner.hasInputQueue(this.inputQueue));
    }

    @Test
    public void testAddAndRemoveByTimeout() throws InterruptedException, CombinerException {
        this.messageCombiner.addInputQueue(this.inputQueue, 10d, 10, TimeUnit.MILLISECONDS);

        assertTrue(this.messageCombiner.hasInputQueue(this.inputQueue));

        Thread.sleep(20);

        assertFalse(this.messageCombiner.hasInputQueue(this.inputQueue));
    }

    @Test(expectedExceptions = CombinerException.class, expectedExceptionsMessageRegExp = "input queue not present")
    public void testRemoveNotExisting() throws CombinerException {
        this.messageCombiner.removeInputQueue(this.inputQueue);
        fail();
    }

    @Test
    public void testAddAndRemoveTwoQueues() throws CombinerException {
        this.messageCombiner.addInputQueue(this.inputQueue, 10d, 10, TimeUnit.SECONDS);
        LinkedBlockingQueue<TestMessage> queue = new LinkedBlockingQueue<>();
        this.messageCombiner.addInputQueue(queue, 5, 5, TimeUnit.SECONDS);

        assertTrue(this.messageCombiner.hasInputQueue(this.inputQueue));
        assertTrue(this.messageCombiner.hasInputQueue(queue));

        this.messageCombiner.removeInputQueue(queue);

        assertTrue(this.messageCombiner.hasInputQueue(this.inputQueue));
        assertFalse(this.messageCombiner.hasInputQueue(queue));

        this.messageCombiner.removeInputQueue(this.inputQueue);

        assertFalse(this.messageCombiner.hasInputQueue(this.inputQueue));
        assertFalse(this.messageCombiner.hasInputQueue(queue));
    }
}
