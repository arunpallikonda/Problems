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

    String getPrimaryKey(T obj);

    default int getDecimalPrecision(String fieldPath) {
        return 5;
    }

    default Set<String> fieldsToIgnore() {
        return Set.of();
    }

    default Runnable createStreamCompareTask(
            BlockingQueue<T> source1Queue,
            BlockingQueue<T> source2Queue,
            ReportService reportService,
            String schemaName
    ) {
        return () -> {
            try {
                Map<String, T> cache1 = new HashMap<>();
                Map<String, T> cache2 = new HashMap<>();

                while (!Thread.currentThread().isInterrupted()) {
                    T obj1 = source1Queue.poll();
                    T obj2 = source2Queue.poll();

                    if (obj1 != null) {
                        String pk = getPrimaryKey(obj1);
                        cache1.put(pk, obj1);
                        if (cache2.containsKey(pk)) {
                            compare(cache1.remove(pk), cache2.remove(pk), reportService, schemaName);
                        }
                    }

                    if (obj2 != null) {
                        String pk = getPrimaryKey(obj2);
                        cache2.put(pk, obj2);
                        if (cache1.containsKey(pk)) {
                            compare(cache1.remove(pk), cache2.remove(pk), reportService, schemaName);
                        }
                    }
                }

                for (Map.Entry<String, T> entry : cache1.entrySet()) {
                    reportService.submitDifference(schemaName, new Difference(
                            entry.getKey(), "ALL_FIELDS", entry.getValue(), null, DifferenceType.MISSING_ROW
                    ));
                }

            } catch (Exception e) {
                LOGGER.error("Error in comparison stream task", e);
            }
        };
    }

    default void compare(T a, T b, ReportService reportService, String schemaName) {
        String primaryKey = getPrimaryKey(a);
        Map<String, Object> fieldsA = extractFields(a, "");
        Map<String, Object> fieldsB = extractFields(b, "");

        for (String field : fieldsA.keySet()) {
            if (fieldsToIgnore().contains(field)) continue;

            Object val1 = fieldsA.get(field);
            Object val2 = fieldsB.get(field);

            if (!Objects.equals(format(val1, field), format(val2, field))) {
                reportService.submitDifference(schemaName, new Difference(
                        primaryKey, field, val1, val2, DifferenceType.VALUE_MISMATCH
                ));
            }
        }
    }

    default Map<String, Object> extractFields(Message msg, String path) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (Descriptors.FieldDescriptor fd : msg.getDescriptorForType().getFields()) {
            Object value = msg.getField(fd);
            String fullPath = path.isEmpty() ? fd.getName() : path + "." + fd.getName();

            if (fd.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE && value instanceof Message) {
                map.putAll(extractFields((Message) value, fullPath));
            } else {
                map.put(fullPath, value);
            }
        }
        return map;
    }

    default Object format(Object value, String fieldPath) {
        if (value instanceof com.google.protobuf.DoubleValue dv) {
            return String.format("%1$." + getDecimalPrecision(fieldPath) + "f", dv.getValue());
        }
        return value;
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
