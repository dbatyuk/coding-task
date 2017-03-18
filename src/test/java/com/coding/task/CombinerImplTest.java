package com.coding.task;

import com.coding.task.Combiner.CombinerException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.fail;

/**
 * Created by dbatyuk on 18.03.2017.
 */
public class CombinerImplTest {

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "outputQueue can't be null")
    public void testCreateNullOutputQueue() {
        CombinerImpl.createStarted(null);
        fail();
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "combiner can't be null")
    public void testStopNullCombiner() {
        CombinerImpl.stop(null);
        fail();
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "combiner should be instance of CombinerImpl")
    public void testStopNotCombinerImpl() {
        Combiner combiner = Mockito.mock(Combiner.class);
        CombinerImpl.stop(combiner);
        fail();
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "combiner already stopped")
    public void testStopAlreadyStopped() {
        SynchronousQueue<TestMessage> synchronousQueue = new SynchronousQueue<>();

        Combiner combiner = CombinerImpl.createStarted(synchronousQueue);
        CombinerImpl.stop(combiner);
        CombinerImpl.stop(combiner);
        fail();
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "queue can't be null")
    public void testAddQueueNull() throws CombinerException {
        SynchronousQueue<TestMessage> synchronousQueue = new SynchronousQueue<>();

        Combiner combiner = CombinerImpl.createStarted(synchronousQueue);
        combiner.addInputQueue(null, 10, 20, TimeUnit.DAYS);
        fail();
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "priority should be more than 0")
    public void testAddNegativePriority() throws CombinerException {
        SynchronousQueue<TestMessage> synchronousQueue = new SynchronousQueue<>();

        BlockingQueue<TestMessage> blockingQueue = new LinkedBlockingQueue<>();
        Combiner combiner = CombinerImpl.createStarted(synchronousQueue);
        combiner.addInputQueue(blockingQueue, -1, 20, TimeUnit.DAYS);
        fail();
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "isEmptyTimeout should be more than 0")
    public void testAddZeroTimeout() throws CombinerException {
        SynchronousQueue<TestMessage> synchronousQueue = new SynchronousQueue<>();

        BlockingQueue<TestMessage> blockingQueue = new LinkedBlockingQueue<>();
        Combiner combiner = CombinerImpl.createStarted(synchronousQueue);
        combiner.addInputQueue(blockingQueue, 5, 0, TimeUnit.DAYS);
        fail();
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "timeUnit can't be null")
    public void testAddNullTimeUnit() throws CombinerException {
        SynchronousQueue<TestMessage> synchronousQueue = new SynchronousQueue<>();

        BlockingQueue<TestMessage> blockingQueue = new LinkedBlockingQueue<>();
        Combiner combiner = CombinerImpl.createStarted(synchronousQueue);
        combiner.addInputQueue(blockingQueue, 5, 10, null);
        fail();
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "queue can't be null")
    public void testRemoveQueueNull() throws CombinerException {
        SynchronousQueue<TestMessage> synchronousQueue = new SynchronousQueue<>();

        Combiner combiner = CombinerImpl.createStarted(synchronousQueue);
        combiner.removeInputQueue(null);
        fail();
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "queue can't be null")
    public void testHasQueueNull() throws CombinerException {
        SynchronousQueue<TestMessage> synchronousQueue = new SynchronousQueue<>();

        Combiner combiner = CombinerImpl.createStarted(synchronousQueue);
        combiner.hasInputQueue(null);
        fail();
    }
}