// === File: Difference.java ===
package com.example.compare;

public class Difference {
    String primaryKey;
    String fieldName;
    Object value1;
    Object value2;
    DifferenceType type;

    public Difference(String pk, String field, Object v1, Object v2, DifferenceType type) {
        this.primaryKey = pk;
        this.fieldName = field;
        this.value1 = v1;
        this.value2 = v2;
        this.type = type;
    }

    public String toCSV() {
        return String.join(",",
                escape(primaryKey),
                escape(fieldName),
                escape(String.valueOf(value1)),
                escape(String.valueOf(value2)),
                type.name()
        );
    }

    private String escape(String s) {
        return s == null ? "" : s.replaceAll(",", "\\,");
    }
}

// === File: DifferenceType.java ===
package com.example.compare;

public enum DifferenceType {
    VALUE_MISMATCH,
    MISSING_ROW
}

// === File: ReportService.java ===
package com.example.compare;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.*;

public class ReportService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReportService.class);
    private final Map<String, BlockingQueue<Difference>> schemaQueues = new ConcurrentHashMap<>();
    private final Map<String, FileWriter> writers = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final String outputDir;

    public ReportService(String outputDir) {
        this.outputDir = outputDir;
        new File(outputDir).mkdirs();
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    public void submitDifference(String schemaName, Difference diff) {
        schemaQueues.computeIfAbsent(schemaName, name -> {
            BlockingQueue<Difference> queue = new LinkedBlockingQueue<>();
            executor.submit(() -> consume(name, queue));
            return queue;
        }).offer(diff);
    }

    private void consume(String schemaName, BlockingQueue<Difference> queue) {
        try {
            File file = Paths.get(outputDir, schemaName + "_diff_report.csv").toFile();
            boolean writeHeader = !file.exists();
            FileWriter writer = new FileWriter(file, true);
            writers.put(schemaName, writer);

            if (writeHeader) {
                writer.write("primaryKey,fieldName,value1,value2,differenceType\n");
            }

            while (true) {
                Difference diff = queue.poll(5, TimeUnit.SECONDS);
                if (diff != null) {
                    LOGGER.info("[{}] Writing difference: {}", schemaName, diff);
                    writer.write(diff.toCSV() + "\n");
                    writer.flush();
                }
            }
        } catch (Exception e) {
            LOGGER.error("[{}] Error writing report", schemaName, e);
        }
    }

    private void shutdown() {
        LOGGER.info("Shutting down ReportService");
        writers.forEach((schema, writer) -> {
            try {
                writer.close();
            } catch (IOException e) {
                LOGGER.error("Error closing writer for schema: {}", schema, e);
            }
        });
        executor.shutdownNow();
    }
}

// === File: CompareService.java ===
package com.example.compare;

import com.google.protobuf.Message;
import com.google.protobuf.Descriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;

public interface CompareService<T extends Message> {
    Logger LOGGER = LoggerFactory.getLogger(CompareService.class);

    // Abstract method to be implemented by each proto-specific service
    String getPrimaryKey(T obj);

    // Fields to exclude from comparison
    default Set<String> fieldsToIgnore() {
        return Set.of();
    }

    // Custom decimal precision per field
    default int getDecimalPrecision(String fieldPath) {
        return 5;
    }

    // Custom logic for specific fields
    default BiPredicate<Object, Object> customComparisonLogic(String fieldPath) {
        return null;
    }

    // Core method for batch processing with idle timeout
    default Runnable createBatchCompareTask(
            BlockingQueue<T> source1Queue,
            BlockingQueue<T> source2Queue,
            ReportService reportService,
            String schemaName,
            Duration idleTimeout
    ) {
        return () -> {
            Map<String, T> source1Map = new HashMap<>();
            Map<String, T> source2Map = new HashMap<>();
            Instant lastUpdate = Instant.now();

            while (true) {
                boolean dataReceived = false;
                T s1 = source1Queue.poll();
                T s2 = source2Queue.poll();

                if (s1 != null) {
                    source1Map.put(getPrimaryKey(s1), s1);
                    lastUpdate = Instant.now();
                    dataReceived = true;
                    LOGGER.debug("Received from source1: {}", getPrimaryKey(s1));
                }
                if (s2 != null) {
                    source2Map.put(getPrimaryKey(s2), s2);
                    lastUpdate = Instant.now();
                    dataReceived = true;
                    LOGGER.debug("Received from source2: {}", getPrimaryKey(s2));
                }

                // Compare when both sides contain a key
                Iterator<String> iter = source1Map.keySet().iterator();
                while (iter.hasNext()) {
                    String key = iter.next();
                    if (source2Map.containsKey(key)) {
                        T obj1 = source1Map.get(key);
                        T obj2 = source2Map.remove(key);
                        iter.remove();
                        List<Difference> diffs = compare(obj1, obj2);
                        for (Difference d : diffs) {
                            reportService.submitDifference(schemaName, d);
                        }
                    }
                }

                if (!dataReceived && Duration.between(lastUpdate, Instant.now()).compareTo(idleTimeout) > 0) {
                    LOGGER.info("Idle timeout reached. Writing unmatched rows.");
                    for (T unmatched : source1Map.values()) {
                        reportService.submitDifference(schemaName,
                                new Difference(getPrimaryKey(unmatched), "-", toJson(unmatched), "", DifferenceType.MISSING_ROW));
                    }
                    return;
                }

                try {
                    Thread.sleep(200);
                } catch (InterruptedException ignored) {
                    return;
                }
            }
        };
    }

    default List<Difference> compare(T source1, T source2) {
        List<Difference> result = new ArrayList<>();
        flattenAndCompare("", source1, source2, result);
        return result;
    }

    private void flattenAndCompare(String prefix, Message m1, Message m2, List<Difference> result) {
        for (Descriptors.FieldDescriptor field : m1.getDescriptorForType().getFields()) {
            String path = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
            if (fieldsToIgnore().contains(path)) continue;

            Object v1 = m1.hasField(field) ? m1.getField(field) : null;
            Object v2 = m2.hasField(field) ? m2.getField(field) : null;

            if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE
                    && v1 instanceof Message && v2 instanceof Message) {
                flattenAndCompare(path, (Message) v1, (Message) v2, result);
            } else {
                BiPredicate<Object, Object> custom = customComparisonLogic(path);
                boolean match;
                if (custom != null) {
                    match = custom.test(v1, v2);
                } else if (v1 instanceof Double || v1 instanceof Float) {
                    match = Objects.equals(roundDouble(v1, getDecimalPrecision(path)),
                            roundDouble(v2, getDecimalPrecision(path)));
                } else {
                    match = Objects.equals(v1, v2);
                }

                if (!match) {
                    result.add(new Difference(getPrimaryKey((T) m1), path, v1, v2, DifferenceType.VALUE_MISMATCH));
                }
            }
        }
    }

    private Object roundDouble(Object val, int precision) {
        if (val instanceof Double d)
            return Math.round(d * Math.pow(10, precision)) / Math.pow(10, precision);
        if (val instanceof Float f)
            return Math.round(f * Math.pow(10, precision)) / Math.pow(10, precision);
        return val;
    }

    private String toJson(Message m) {
        try {
            return JsonFormat.printer().print(m);
        } catch (Exception e) {
            return m.toString();
        }
    }
}

// === File: PriceDataCompareService.java ===
package com.example.compare;

import com.example.proto.PriceData;
import java.util.Set;

public class PriceDataCompareService implements CompareService<PriceData> {
    @Override
    public String getPrimaryKey(PriceData obj) {
        return obj.getId();
    }

    @Override
    public int getDecimalPrecision(String fieldPath) {
        return fieldPath.endsWith("price") ? 4 : 5;
    }

    @Override
    public Set<String> fieldsToIgnore() {
        return Set.of("timestamp", "details.meta.source");
    }
}

// === File: PriceDataExample.java ===
package com.example.compare;

import com.example.proto.PriceData;
import com.google.protobuf.*;
import com.google.protobuf.util.Timestamps;
import java.util.concurrent.*;

public class PriceDataExample {
    public static void main(String[] args) throws Exception {
        String outputDir = "./output";
        String schemaName = "PriceData";

        BlockingQueue<PriceData> priceCacheQueue = new LinkedBlockingQueue<>();
        BlockingQueue<PriceData> apiResponseQueue = new LinkedBlockingQueue<>();

        Timestamp now = Timestamps.fromMillis(System.currentTimeMillis());

        PriceData source1 = PriceData.newBuilder()
                .setId("ABC123")
                .setPrice(DoubleValue.of(123.456789))
                .setAvailable(BoolValue.of(true))
                .setCurrency(StringValue.of("USD"))
                .setTimestamp(now)
                .setDetails(PriceData.Details.newBuilder()
                        .setRegion("US")
                        .setNotes("Preferred Vendor")
                        .build())
                .build();

        PriceData source2 = PriceData.newBuilder()
                .setId("ABC123")
                .setPrice(DoubleValue.of(123.4567))
                .setAvailable(BoolValue.of(true))
                .setCurrency(StringValue.of("USD"))
                .setTimestamp(now)
                .setDetails(PriceData.Details.newBuilder()
                        .setRegion("EU")
                        .setNotes("Preferred Vendor")
                        .build())
                .build();

        priceCacheQueue.add(source1);
        apiResponseQueue.add(source2);

        ReportService reportService = new ReportService(outputDir);
        PriceDataCompareService compareService = new PriceDataCompareService();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(compareService.createStreamCompareTask(priceCacheQueue, apiResponseQueue, reportService, schemaName));

        Thread.sleep(5000);
        executor.shutdownNow();
    }
}







// === File: DifferenceType.java ===
package com.example.compare;

public enum DifferenceType {
    VALUE_MISMATCH,
    MISSING_ROW
}

// === File: Difference.java ===
package com.example.compare;

public record Difference(
    String primaryKey,
    String fieldPath,
    Object value1,
    Object value2,
    DifferenceType type
) {}

// === File: ReportService.java ===
package com.example.compare;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class ReportService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReportService.class);
    private final Map<String, BlockingQueue<Difference>> queues = new ConcurrentHashMap<>();
    private final Map<String, BufferedWriter> writers = new ConcurrentHashMap<>();
    private final ExecutorService writerExecutor = Executors.newCachedThreadPool();
    private final String outputDir;

    public ReportService(String outputDir) {
        this.outputDir = outputDir;
        try {
            Files.createDirectories(Paths.get(outputDir));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create output dir", e);
        }
    }

    public void submitDifference(String schema, Difference diff) {
        queues.computeIfAbsent(schema, this::startWriter).add(diff);
    }

    private BlockingQueue<Difference> startWriter(String schema) {
        BlockingQueue<Difference> q = new LinkedBlockingQueue<>();
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputDir + "/" + schema + "_diff.csv"));
            writer.write("primaryKey,fieldPath,value1,value2,differenceType\n");
            writers.put(schema, writer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        writerExecutor.submit(() -> {
            try {
                while (true) {
                    Difference d = q.poll(60, TimeUnit.SECONDS);
                    if (d == null) break;
                    writers.get(schema).write(String.format("%s,%s,%s,%s,%s\n",
                        d.primaryKey(), d.fieldPath(), d.value1(), d.value2(), d.type()));
                    writers.get(schema).flush();
                }
                writers.get(schema).close();
            } catch (Exception ex) {
                LOGGER.error("Error writing report for {}", schema, ex);
            }
        });
        return q;
    }
}

// === File: PriceDataCompareService.java ===
package com.example.compare;

import com.example.proto.PriceData;
import com.google.protobuf.Message;

import java.util.Set;

public class PriceDataCompareService implements CompareService<PriceData> {
    @Override
    public String getPrimaryKey(PriceData obj) {
        return obj.getId();
    }

    @Override
    public Set<String> fieldsToIgnore() {
        return Set.of("timestamp", "details.notes");
    }

    @Override
    public int getDecimalPrecision(String fieldPath) {
        return switch (fieldPath) {
            case "price" -> 4;
            default -> 5;
        };
    }
}

// === File: ProtoBatchCompareApplication.java ===
package com.example.compare;

import com.example.proto.PriceData;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;

public class ProtoBatchCompareApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProtoBatchCompareApplication.class);

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        String outputDir = "./output";
        String schemaName = "PriceData";

        BlockingQueue<PriceData> source1Queue = new LinkedBlockingQueue<>();
        BlockingQueue<PriceData> source2Queue = new LinkedBlockingQueue<>();

        populateTestData(source1Queue, source2Queue);

        ReportService reportService = new ReportService(outputDir);
        PriceDataCompareService compareService = new PriceDataCompareService();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> compareFuture = executor.submit(
            compareService.createBatchCompareTask(
                source1Queue, source2Queue, reportService, schemaName, Duration.ofSeconds(60))
        );

        compareFuture.get();
        executor.shutdown();
        LOGGER.info("Comparison complete. Application exiting.");
    }

    private static void populateTestData(BlockingQueue<PriceData> q1, BlockingQueue<PriceData> q2) {
        Timestamp now = Timestamps.fromMillis(Instant.now().toEpochMilli());

        PriceData a = PriceData.newBuilder()
            .setId("id-1")
            .setAvailable(com.google.protobuf.BoolValue.of(true))
            .setPrice(com.google.protobuf.DoubleValue.of(123.4567))
            .setCurrency(com.google.protobuf.StringValue.of("USD"))
            .setTimestamp(now)
            .setDetails(PriceData.Details.newBuilder().setRegion("US").build())
            .build();

        PriceData b = PriceData.newBuilder()
            .setId("id-1")
            .setAvailable(com.google.protobuf.BoolValue.of(true))
            .setPrice(com.google.protobuf.DoubleValue.of(123.45999))
            .setCurrency(com.google.protobuf.StringValue.of("USD"))
            .setTimestamp(now)
            .setDetails(PriceData.Details.newBuilder().setRegion("EU").build())
            .build();

        PriceData c = PriceData.newBuilder()
            .setId("id-2")
            .setAvailable(com.google.protobuf.BoolValue.of(false))
            .setPrice(com.google.protobuf.DoubleValue.of(99.99))
            .setCurrency(com.google.protobuf.StringValue.of("EUR"))
            .setTimestamp(now)
            .setDetails(PriceData.Details.newBuilder().setRegion("EU").build())
            .build();

        q1.add(a);
        q1.add(c);
        q2.add(b);
    }
}
