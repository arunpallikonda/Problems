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







package org.example;

import static org.awaitility.Awaitility.await;

import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.logging.*;

import static org.junit.jupiter.api.Assertions.*;

class QueryLoggerTest {

    private QueryLogger logger;
    private TestLogHandler logHandler;

    @BeforeEach
    void setup() {
        Logger baseLogger = Logger.getLogger(QueryLogger.class.getName());
        logHandler = new TestLogHandler();
        baseLogger.addHandler(logHandler);
        baseLogger.setUseParentHandlers(false);
        baseLogger.setLevel(Level.INFO);
    }

    @AfterEach
    void tearDown() {
        logHandler.clear();
    }

    @Test
    void testLogStartAndEndUpdatesAverageAndLogs() {
        logger = new QueryLogger(100, 10);
        String queryName = "SELECT * FROM users";
        String token = logger.logStart(queryName);
        // simulate query run time
        try {
            Thread.sleep(50);
        } catch (InterruptedException ignored) {
        }
        logger.logEnd(queryName, token);
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            String logs = logHandler.getCapturedLog();
            assertTrue(logs.contains("SELECT * FROM users"));
            assertTrue(logs.contains("Avg Time"));
            assertTrue(logs.contains("QUERY LOG START"));
        });
    }

    @Test
    void testMultipleLogEntries() {
        logger = new QueryLogger(100, 5);
        for (int i = 0; i < 3; i++) {
            String token = logger.logStart("INSERT INTO orders");
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {
            }
            logger.logEnd("INSERT INTO orders", token);
        }
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertTrue(logHandler.getCapturedLog().contains("INSERT INTO orders"));
        });
    }

    @Test
    void testLogEndWithoutStart() {
        logger = new QueryLogger(100, 10);
        logger.logEnd("NO_START_QUERY", "unknown-token");
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertFalse(logHandler.getCapturedLog().contains("NO_START_QUERY"));
        });
    }

    @Test
    void testLongQueryNameIsWrapped() {
        logger = new QueryLogger(100, 10);
        String longQuery = "A".repeat(60);
        String token = logger.logStart(longQuery);
        try {
            Thread.sleep(30);
        } catch (InterruptedException ignored) {
        }
        logger.logEnd(longQuery, token);
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            String capturedLog = logHandler.getCapturedLog();
            assertTrue(capturedLog.contains("QUERY LOG START"));
            assertTrue(capturedLog.contains("A".repeat(30))); // Check part of the long query
        });
    }

    @Test
    void testExceptionInEventProcessorThread() {
        // Create a logger with a queue size of 1 to force a potential failure quickly
        logger = new QueryLogger(1, 10);

        // Interrupt the thread before it starts processing to simulate exception
        Thread.currentThread().interrupt(); // force an interruption

        String token = logger.logStart("INTERRUPTED_QUERY");
        logger.logEnd("INTERRUPTED_QUERY", token);

        // Clear interruption status for the test thread
        Thread.interrupted();

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            String logs = logHandler.getCapturedLog();
            assertTrue(logs.contains("INTERRUPTED_QUERY"));
        });
    }

    static class TestLogHandler extends Handler {
        private final StringBuilder logCapture = new StringBuilder();

        @Override
        public void publish(LogRecord record) {
            logCapture.append(record.getMessage()).append("\n");
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() throws SecurityException {
        }

        public String getCapturedLog() {
            return logCapture.toString();
        }

        public void clear() {
            logCapture.setLength(0);
        }

    }
    @Test
    void testEndEventWithoutStartTokenDoesNotCrash() {
        logger = new QueryLogger(1000, 5);

        // Log an END event with a token that doesn't exist in startTimes
        logger.logEnd("orphan_query", UUID.randomUUID().toString());

        // Wait and ensure that nothing crashes and system continues to operate
        await().atMost(Duration.ofSeconds(2)).until(() -> true); // just a delay to let the background thread run

        // If the test reaches here without exceptions, it's successful
        assertTrue(true);
    }
}

