package com.coding.task;

import com.google.common.base.Stopwatch;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by dbatyuk on 27.02.2017.
 */
public class CombinerImpl<T> extends Combiner<T> implements Runnable {

    private final Map<BlockingQueue<T>, QueueConfig> queueMap = new HashMap<>();

    private double minPriority = Double.MAX_VALUE;

    private CombinerImpl(SynchronousQueue<T> outputQueue) {
        super(outputQueue);
    }

    public static <T> Combiner<T> createStarted(SynchronousQueue<T> outputQueue) throws CombinerException {
        CombinerImpl<T> combiner = new CombinerImpl<>(outputQueue);
        new Thread(combiner).start();

        return combiner;
    }

    @Override
    public void run() {
        while (true) {
            List<BlockingQueue<T>> queuesToRemove = new ArrayList<>();

            for (Iterator<Entry<BlockingQueue<T>, QueueConfig>> it = this.queueMap.entrySet().iterator(); it.hasNext(); ) {
                Entry<BlockingQueue<T>, QueueConfig> queueEntry = it.next();
                BlockingQueue<T> queue = queueEntry.getKey();
                QueueConfig queueConfig = queueEntry.getValue();

                int iterations = getIterations(queueConfig.getPriority());

                for (int i = 0; i < iterations; i++) {
                    T msg = queue.poll();

                    if (msg == null) {
                        if (this.checkIfNeedToRemove(queueConfig)) {
                            queuesToRemove.add(queue);
                        }
                        break;
                    }

                    queueConfig.getTimer().reset();

                    try {
                        this.outputQueue.put(msg);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            for (BlockingQueue<T> queue : queuesToRemove) {
                try {
                    this.removeInputQueue(queue);
                } catch (CombinerException e) {
                    e.printStackTrace();
                }
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
        synchronized (this) {
            this.queueMap.put(queue, new QueueConfig(priority, isEmptyTimeout, timeUnit));

            this.minPriority = Math.min(this.minPriority, priority);
        }
    }

    @Override
    public void removeInputQueue(BlockingQueue<T> queue) throws CombinerException {
        synchronized (this) {
            QueueConfig queueConfig = this.queueMap.get(queue);
            this.queueMap.remove(queue);

            if (this.minPriority == queueConfig.priority) {
                for (QueueConfig qc : this.queueMap.values()) {
                    this.minPriority = Math.min(this.minPriority, qc.priority);
                }
            }

        }
    }

    @Override
    public boolean hasInputQueue(BlockingQueue<T> queue) {
        return this.queueMap.containsKey(queue);
    }

    private static class QueueConfig {

        private final double priority;
        private final long isEmptyTimeout;
        private final TimeUnit timeUnit;

        private final Stopwatch timer = Stopwatch.createUnstarted();

        private QueueConfig(double priority, long isEmptyTimeout, TimeUnit timeUnit) {
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
