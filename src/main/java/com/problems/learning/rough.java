package org.example;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class QueryLogger {

    private static final Logger logger = Logger.getLogger(QueryLogger.class.getName());
    private final BlockingQueue<QueryEvent> eventQueue = new LinkedBlockingQueue<>();
    private final ConcurrentMap<String, QueryStats> queryStatsMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Instant> startTimes = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public QueryLogger(long logIntervalMs, int maxQueriesToLog) {
        scheduler.scheduleAtFixedRate(() -> logAverages(maxQueriesToLog), logIntervalMs, logIntervalMs, TimeUnit.MILLISECONDS);
        startEventProcessor();
    }

    public String logStart(String queryName) {
        String token = UUID.randomUUID().toString();
        startTimes.put(token, Instant.now());
        eventQueue.offer(new QueryEvent(queryName, token, EventType.START));
        return token;
    }

    public void logEnd(String queryName, String token) {
        eventQueue.offer(new QueryEvent(queryName, token, EventType.END));
    }

    private void startEventProcessor() {
        Thread thread = new Thread(() -> {
            while (true) {
                try {
                    QueryEvent event = eventQueue.take();
                    if (event.type == EventType.START) {
                        // Already recorded in startTimes
                    } else if (event.type == EventType.END) {
                        Instant start = startTimes.remove(event.token);
                        if (start != null) {
                            long duration = Duration.between(start, Instant.now()).toMillis();
                            queryStatsMap.computeIfAbsent(event.queryName, k -> new QueryStats()).add(duration);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    private void logAverages(int maxQueriesToLog) {
        StringBuilder sb = new StringBuilder();
        sb.append("########################## QUERY LOG START ##########################\n");
        sb.append(String.format("%-35s | %-10s | %-10s%n", "Query", "Avg Time", "Count"));
        sb.append("-------------------------------------+------------+------------\n");

        queryStatsMap.entrySet().stream()
            .sorted((a, b) -> Long.compare(b.getValue().getAvgTime(), a.getValue().getAvgTime()))
            .limit(maxQueriesToLog)
            .forEach(entry -> {
                String queryName = entry.getKey();
                long avgTime = entry.getValue().getAvgTime();
                long count = entry.getValue().count.get();
                if (queryName.length() > 35) {
                    sb.append(String.format("%-35s | %-10s | %-10s%n", queryName.substring(0, 35), "", ""));
                    sb.append(String.format("%-35s | %-10d | %-10d%n", queryName.substring(35), avgTime, count));
                } else {
                    sb.append(String.format("%-35s | %-10d | %-10d%n", queryName, avgTime, count));
                }
            });

        sb.append("########################### QUERY LOG END ###########################");
        logger.info(sb.toString());
    }

    private static class QueryEvent {
        final String queryName;
        final String token;
        final EventType type;

        QueryEvent(String queryName, String token, EventType type) {
            this.queryName = queryName;
            this.token = token;
            this.type = type;
        }
    }

    private enum EventType {
        START, END
    }

    private static class QueryStats {
        private final AtomicLong totalTime = new AtomicLong();
        private final AtomicLong count = new AtomicLong();

        void add(long time) {
            totalTime.addAndGet(time);
            count.incrementAndGet();
        }

        long getAvgTime() {
            long cnt = count.get();
            return cnt == 0 ? 0 : totalTime.get() / cnt;
        }
    }
}
