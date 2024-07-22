import java.util.*;
import java.util.concurrent.*;

public class MultiThreadedQueueProcessor {

    private static final int QUEUE_CAPACITY = 100;
    private static final int NUM_PRODUCERS = 5;
    private static final int NUM_CONSUMERS = 5;

    public static void main(String[] args) throws InterruptedException {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        List<List<String>> lists = Collections.synchronizedList(new ArrayList<>());

        ExecutorService producerExecutor = Executors.newFixedThreadPool(NUM_PRODUCERS);
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(NUM_CONSUMERS);

        // Create and start producer threads
        for (int i = 0; i < NUM_PRODUCERS; i++) {
            producerExecutor.submit(new Producer(queue, "Producer-" + i));
        }

        // Create and start consumer threads
        for (int i = 0; i < NUM_CONSUMERS; i++) {
            consumerExecutor.submit(new Consumer(queue, lists));
        }

        // Shutdown executors after some time
        producerExecutor.shutdown();
        consumerExecutor.shutdown();

        producerExecutor.awaitTermination(1, TimeUnit.MINUTES);
        consumerExecutor.awaitTermination(1, TimeUnit.MINUTES);

        // Print the results
        for (List<String> list : lists) {
            System.out.println("List size: " + list.size());
        }
    }

    static class Producer implements Runnable {
        private final BlockingQueue<String> queue;
        private final String name;

        Producer(BlockingQueue<String> queue, String name) {
            this.queue = queue;
            this.name = name;
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < 100; i++) {
                    String data = name + "-Data-" + i;
                    queue.put(data);
                    System.out.println(name + " produced: " + data);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    static class Consumer implements Runnable {
        private final BlockingQueue<String> queue;
        private final List<List<String>> lists;

        Consumer(BlockingQueue<String> queue, List<List<String>> lists) {
            this.queue = queue;
            this.lists = lists;
        }

        @Override
        public void run() {
            List<String> localList = new ArrayList<>();
            try {
                while (true) {
                    String data = queue.take();
                    localList.add(data);
                    System.out.println(Thread.currentThread().getName() + " consumed: " + data);
                    if (localList.size() >= 50) {
                        synchronized (lists) {
                            lists.add(new ArrayList<>(localList));
                        }
                        localList.clear();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
