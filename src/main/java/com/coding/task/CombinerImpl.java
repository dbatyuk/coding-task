package com.coding.task;

import java.util.List;
import java.util.concurrent.*;

/**
 * Created by dbatyuk on 27.02.2017.
 */
public class CombinerImpl<T> extends Combiner<T> {

    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final List<QueueContainer> queueContainers = new CopyOnWriteArrayList<>();

    private CombinerImpl(SynchronousQueue<T> outputQueue) {
        super(outputQueue);
    }

    public static <T> Combiner<T> createAndStart(SynchronousQueue<T> outputQueue){
        CombinerImpl<T> combiner = new CombinerImpl<>(outputQueue);
        combiner.run();
        return combiner;
    }

    public void run(){
        while (true){
            try {

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void addInputQueue(BlockingQueue<T> queue, double priority, long isEmptyTimeout, TimeUnit timeUnit) throws CombinerException {
        queueContainers.add(new QueueContainer(queue, priority, isEmptyTimeout, timeUnit));
    }

    public void removeInputQueue(BlockingQueue<T> queue) throws CombinerException {

    }

    public boolean hasInputQueue(BlockingQueue<T> queue) {
        return false;
    }

    private int getIterations(){

    }

    private class QueueContainer {
        private final BlockingQueue<T> queue;
        private final double priority;
        private final long isEmptyTimeout;
        private final TimeUnit timeUnit;

        private QueueContainer(BlockingQueue<T> queue, double priority, long isEmptyTimeout, TimeUnit timeUnit) {
            this.queue = queue;
            this.priority = priority;
            this.isEmptyTimeout = isEmptyTimeout;
            this.timeUnit = timeUnit;
        }

        public BlockingQueue<T> getQueue() {
            return queue;
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
    }

    private class InputQueueTask implements Callable<Boolean>{

        private final BlockingQueue<T> queue;
        private final int iterations;
        private final long isEmptyTimeout;
        private final TimeUnit timeUnit;

        private InputQueueTask(BlockingQueue<T> queue, int iterations, long isEmptyTimeout, TimeUnit timeUnit) {
            this.queue = queue;
            this.iterations = iterations;
            this.isEmptyTimeout = isEmptyTimeout;
            this.timeUnit = timeUnit;
        }

        @Override
        public Boolean call() throws Exception {

            for(int i =0; i < this.iterations; i ++){

                T message = queue.poll(isEmptyTimeout, timeUnit);

                if(message == null){
                    removeInputQueue(queue);
                    return false;
                }

                outputQueue.put(message);
            }

            return true;
        }
    }
}
