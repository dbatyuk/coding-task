package com.coding.task;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by dbatyuk on 27.02.2017.
 */
public class CombinerImpl<T> extends Combiner<T> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(CombinerImpl.class);

    private final Map<BlockingQueue<T>, QueueConfig> queueMap = new HashMap<>();

    private final AtomicBoolean running = new AtomicBoolean(false);

    private double minPriority = Double.MAX_VALUE;

    private CombinerImpl(SynchronousQueue<T> outputQueue) {
        super(outputQueue);
    }

    public static <T> Combiner<T> createStarted(SynchronousQueue<T> outputQueue) {
        Preconditions.checkArgument(outputQueue != null, "outputQueue can't be null");

        CombinerImpl<T> combinerImpl = new CombinerImpl<>(outputQueue);

        combinerImpl.running.getAndSet(true);
        new Thread(combinerImpl).start();

        return combinerImpl;
    }

    public static <T> void stop(Combiner<T> combiner) {
        Preconditions.checkArgument(combiner != null, "combiner can't be null");
        Preconditions.checkArgument(combiner instanceof CombinerImpl, "combiner should be instance of CombinerImpl");

        CombinerImpl<T> combinerImpl = (CombinerImpl<T>) combiner;

        Preconditions.checkArgument(combinerImpl.running.get(), "combiner already stopped");

        combinerImpl.running.getAndSet(false);
    }

    @Override
    public void run() {
        while (this.running.get()) {
            synchronized (this.queueMap) {
                List<BlockingQueue<T>> queuesToRemove = new ArrayList<>();

                for (Entry<BlockingQueue<T>, QueueConfig> queueEntry : this.queueMap.entrySet()) {
                    BlockingQueue<T> queue = queueEntry.getKey();
                    QueueConfig queueConfig = queueEntry.getValue();

                    int iterations = getIterations(queueConfig.getPriority());

                    for (int i = 0; i < iterations; i++) {
                        T msg = queue.poll();

                        if (msg == null) {
                            if (checkIfNeedToRemove(queueConfig)) {
                                logger.warn("queue will be removed because of timeout, queue {}", queue.hashCode());
                                queuesToRemove.add(queue);
                            }
                            break;
                        }

                        queueConfig.getTimer().reset();

                        try {
                            this.outputQueue.put(msg);
                        } catch (InterruptedException e) {
                            logger.warn("unable to put message {}", msg, e);
                        }
                    }
                }

                queuesToRemove.forEach((queue) -> {
                    try {
                        removeInputQueue(queue);
                    } catch (CombinerException e) {
                        logger.warn("unable to remove input queue {}", queue.hashCode(), e);
                    }
                });
            }
        }
    }

    private boolean checkIfNeedToRemove(QueueConfig queueConfig) {
        Stopwatch timer = queueConfig.getTimer();
        if (timer.isRunning()) {
            long elapsed = timer.elapsed(queueConfig.getTimeUnit());

            return elapsed > queueConfig.getIsEmptyTimeout();
        }

        timer.start();
        return false;
    }

    private int getIterations(double priority) {
        return (int) Math.round(priority / this.minPriority);
    }

    @Override
    public void addInputQueue(BlockingQueue<T> queue, double priority, long isEmptyTimeout, TimeUnit timeUnit) throws CombinerException {
        Preconditions.checkArgument(queue != null, "queue can't be null");

        synchronized (this.queueMap) {
            logger.info("START addInputQueue queue {}, priority {}, isEmptyTimeout {}, timeUnit {}", queue.hashCode(), priority, isEmptyTimeout, timeUnit);
            this.queueMap.put(queue, new QueueConfig(priority, isEmptyTimeout, timeUnit));

            this.minPriority = Math.min(this.minPriority, priority);
            logger.info("END addInputQueue queue {}", queue.hashCode());
        }
    }

    @Override
    public void removeInputQueue(BlockingQueue<T> queue) throws CombinerException {
        Preconditions.checkArgument(queue != null, "queue can't be null");

        synchronized (this.queueMap) {
            logger.info("START removeInputQueue queue {}", queue.hashCode());
            QueueConfig queueConfig = this.queueMap.get(queue);

            if (queueConfig == null) {
                throw new CombinerException("input queue not present");
            }

            this.queueMap.remove(queue);

            if (this.minPriority == queueConfig.priority) {
                for (QueueConfig qc : this.queueMap.values()) {
                    this.minPriority = Math.min(this.minPriority, qc.priority);
                }
            }
            logger.info("FINISH removeInputQueue queue {}", queue.hashCode());
        }
    }

    @Override
    public boolean hasInputQueue(BlockingQueue<T> queue) {
        Preconditions.checkArgument(queue != null, "queue can't be null");

        synchronized (this.queueMap) {
            boolean contains = this.queueMap.containsKey(queue);
            logger.info("hasInputQueue queue {} == {}", queue.hashCode(), contains);
            return contains;
        }
    }

    private static class QueueConfig {

        private final double priority;
        private final long isEmptyTimeout;
        private final TimeUnit timeUnit;

        private final Stopwatch timer = Stopwatch.createUnstarted();

        private QueueConfig(double priority, long isEmptyTimeout, TimeUnit timeUnit) {
            Preconditions.checkArgument(priority > 0, "priority should be more than 0");
            Preconditions.checkArgument(isEmptyTimeout > 0, "isEmptyTimeout should be more than 0");
            Preconditions.checkArgument(timeUnit != null, "timeUnit can't be null");

            this.priority = priority;
            this.isEmptyTimeout = isEmptyTimeout;
            this.timeUnit = timeUnit;
        }

        public double getPriority() {
            return priority;
        }

        public long getIsEmptyTimeout() {
            return isEmptyTimeout;
        }

        public TimeUnit getTimeUnit() {
            return timeUnit;
        }

        public Stopwatch getTimer() {
            return timer;
        }
    }
}
