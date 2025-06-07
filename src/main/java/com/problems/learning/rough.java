package com.example.compare;

import com.google.protobuf.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

public interface CompareService<T extends Message> {

    Logger LOGGER = LoggerFactory.getLogger(CompareService.class);

    String getPrimaryKey(T obj);

    default int getDecimalPrecision(String fieldPath) {
        return 5;
    }

    default List<Difference> compare(T priceCache, T apiResponse) {
        List<Difference> diffs = new ArrayList<>();
        try {
            Map<String, Object> fields1 = extractFields(priceCache, "");
            Map<String, Object> fields2 = extractFields(apiResponse, "");

            Set<String> allKeys = new HashSet<>(fields1.keySet());
            allKeys.addAll(fields2.keySet());

            for (String field : allKeys) {
                Object v1 = fields1.get(field);
                Object v2 = fields2.get(field);

                if (!equalsWithPrecision(field, v1, v2)) {
                    diffs.add(new Difference(getPrimaryKey(priceCache), field, v1, v2, DifferenceType.VALUE_MISMATCH));
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error comparing messages", e);
        }
        return diffs;
    }

    default boolean equalsWithPrecision(String field, Object v1, Object v2) {
        if (v1 == null || v2 == null) return Objects.equals(v1, v2);
        if (v1 instanceof Double || v2 instanceof Double) {
            double d1 = v1 instanceof Double ? (Double) v1 : Double.parseDouble(v1.toString());
            double d2 = v2 instanceof Double ? (Double) v2 : Double.parseDouble(v2.toString());
            int precision = getDecimalPrecision(field);
            String format = "%1$." + precision + "f";
            return String.format(format, d1).equals(String.format(format, d2));
        }
        return Objects.equals(v1, v2);
    }

    default Map<String, Object> extractFields(Message message, String prefix) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
            String fieldName = prefix + entry.getKey().getName();
            Object value = entry.getValue();

            if (value instanceof Message) {
                map.putAll(extractFields((Message) value, fieldName + "."));
            } else {
                map.put(fieldName, value);
            }
        }
        return map;
    }

    default Runnable createStreamCompareTask(
        BlockingQueue<T> priceCacheQueue,
        BlockingQueue<T> apiResponseQueue,
        ReportService reportService,
        String schemaName
    ) {
        return () -> {
            Map<String, T> cacheMap = new HashMap<>();
            Map<String, T> apiMap = new HashMap<>();

            try {
                while (!Thread.currentThread().isInterrupted()) {
                    T priceObj = priceCacheQueue.poll(1, TimeUnit.SECONDS);
                    T apiObj = apiResponseQueue.poll(1, TimeUnit.SECONDS);

                    if (priceObj != null) {
                        String pk = getPrimaryKey(priceObj);
                        LOGGER.info("[{}] Received PriceCache object with key: {}", schemaName, pk);
                        cacheMap.put(pk, priceObj);
                        if (apiMap.containsKey(pk)) {
                            LOGGER.info("[{}] Matching ApiResponseObject found for key: {}", schemaName, pk);
                            List<Difference> diff = compare(priceObj, apiMap.remove(pk));
                            diff.forEach(d -> reportService.submitDifference(schemaName, d));
                            cacheMap.remove(pk);
                        }
                    }

                    if (apiObj != null) {
                        String pk = getPrimaryKey(apiObj);
                        LOGGER.info("[{}] Received ApiResponseObject with key: {}", schemaName, pk);
                        apiMap.put(pk, apiObj);
                        if (cacheMap.containsKey(pk)) {
                            LOGGER.info("[{}] Matching PriceCache object found for key: {}", schemaName, pk);
                            List<Difference> diff = compare(cacheMap.remove(pk), apiObj);
                            diff.forEach(d -> reportService.submitDifference(schemaName, d));
                            apiMap.remove(pk);
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.info("[{}] Comparison task interrupted", schemaName);
            }
        };
    }

    default void flushUnmatched(Map<String, T> source, DifferenceType type, ReportService reportService, String schemaName) {
        for (Map.Entry<String, T> entry : source.entrySet()) {
            LOGGER.info("[{}] Unmatched object with key: {} will be recorded as {}", schemaName, entry.getKey(), type);
            reportService.submitDifference(schemaName, new Difference(entry.getKey(), "", entry.getValue().toString(), null, type));
        }
    }
}

class Difference {
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

enum DifferenceType {
    VALUE_MISMATCH,
    MISSING_ROW
}

class ReportService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReportService.class);
    private final Map<String, BlockingQueue<Difference>> schemaQueues = new ConcurrentHashMap<>();
    private final Map<String, FileWriter> writers = new ConcurrentHashMap<>();
    private final Set<String> initializedSchemas = ConcurrentHashMap.newKeySet();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final String outputDir;

    public ReportService(String outputDir) {
        this.outputDir = outputDir;
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    public void submitDifference(String schemaName, Difference diff) {
        schemaQueues.computeIfAbsent(schemaName, k -> {
            BlockingQueue<Difference> queue = new LinkedBlockingQueue<>();
            executor.submit(() -> consume(schemaName, queue));
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
                    LOGGER.info("[{}] Writing difference to CSV: {}", schemaName, diff);
                    writer.write(diff.toCSV() + "\n");
                    writer.flush();
                }
            }
        } catch (Exception e) {
            LOGGER.error("[{}] Error while writing report", schemaName, e);
        }
    }

    private void shutdown() {
        LOGGER.info("Shutting down report writers");
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








package com.example.compare;

import com.example.proto.PriceData;
import com.google.protobuf.util.Timestamps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.*;

public class ProtoCompareMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProtoCompareMain.class);

    public static void main(String[] args) throws Exception {
        String schemaName = "PriceData";
        String outputDir = "./output";
        new File(outputDir).mkdirs();

        BlockingQueue<PriceData> priceCacheQueue = new LinkedBlockingQueue<>();
        BlockingQueue<PriceData> apiResponseQueue = new LinkedBlockingQueue<>();

        // Populate queues with dummy data
        PriceData price1 = PriceData.newBuilder()
                .setId("ITEM123")
                .setActive(true)
                .setCurrency("USD")
                .setAmount(com.google.protobuf.DoubleValue.of(12.345678))
                .setTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                .setMeta(PriceData.Meta.newBuilder().setSource("internal").build())
                .build();

        PriceData api1 = PriceData.newBuilder()
                .setId("ITEM123")
                .setActive(true)
                .setCurrency("USD")
                .setAmount(com.google.protobuf.DoubleValue.of(12.3456))
                .setTimestamp(price1.getTimestamp())
                .setMeta(PriceData.Meta.newBuilder().setSource("external").build())
                .build();

        priceCacheQueue.offer(price1);
        apiResponseQueue.offer(api1);

        ReportService reportService = new ReportService(outputDir);
        PriceDataCompareService compareService = new PriceDataCompareService();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(compareService.createStreamCompareTask(priceCacheQueue, apiResponseQueue, reportService, schemaName));

        // Allow time for processing
        Thread.sleep(5000);
        executor.shutdownNow();
    }
}

class PriceDataCompareService implements CompareService<PriceData> {
    @Override
    public String getPrimaryKey(PriceData obj) {
        return obj.getId();
    }

    @Override
    public int getDecimalPrecision(String fieldPath) {
        // custom precision logic
        if (fieldPath.endsWith("amount")) return 4;
        return CompareService.super.getDecimalPrecision(fieldPath);
    }
}
