POM

    <dependencies>
  <!-- Kafka Clients -->
  <dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.5.0</version>
  </dependency>

  <!-- Protobuf -->
  <dependency>
    <groupId>com.google.protobuf</groupId>
    <artifactId>protobuf-java</artifactId>
    <version>3.23.4</version>
  </dependency>

  <!-- XML Processing -->
  <dependency>
    <groupId>javax.xml.bind</groupId>
    <artifactId>jaxb-api</artifactId>
    <version>2.3.1</version>
  </dependency>

  <!-- Logging -->
  <dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-simple</artifactId>
    <version>2.0.7</version>
  </dependency>
</dependencies>


Properties

        mode=kafka_to_file_to_kafka
businessDate=2025-04-25
baseDirectory=/path/to/base/directory
topics=topic1,topic2
topic1.className=com.example.protobuf.DdsMessage
topic1.outputTopic=topic1-output
topic2.className=com.example.protobuf.FixmlEquityMessage
topic2.outputTopic=topic2-output


FixmlObjectProcessor

package com.example.util;

import org.w3c.dom.*;
import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;

public class FixmlObjectProcessor {

    public static String updateBizDateInXml(String xmlString, String newBizDate) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc;

        try (InputStream is = new ByteArrayInputStream(xmlString.getBytes())) {
            doc = builder.parse(is);
        }

        Element root = doc.getDocumentElement();
        if (root.hasAttribute("bizdate")) {
            root.setAttribute("bizdate", newBizDate);
        }

        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer transformer = tf.newTransformer();
        StringWriter writer = new StringWriter();
        transformer.transform(new DOMSource(doc), new StreamResult(writer));

        return writer.getBuffer().toString();
    }
}


Application

package com.example;

import com.example.service.KafkaToFileService;
import com.example.service.FileToKafkaService;
import java.util.*;
import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;

public class Application {

    public static void main(String[] args) {
        Properties properties = new Properties();
        try (InputStream input = new FileInputStream("application.properties")) {
            properties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
            return;
        }

        String mode = properties.getProperty("mode");
        String businessDate = properties.getProperty("businessDate");
        String baseDirectory = properties.getProperty("baseDirectory");
        String topicsStr = properties.getProperty("topics");
        String[] topics = topicsStr.split(",");

        Map<String, String> topicClassMap = new HashMap<>();
        Map<String, String> topicOutputMap = new HashMap<>();

        for (String topic : topics) {
            String className = properties.getProperty(topic + ".className");
            String outputTopic = properties.getProperty(topic + ".outputTopic");
            topicClassMap.put(topic, className);
            topicOutputMap.put(topic, outputTopic);
        }

        if ("kafka_to_file_to_kafka".equalsIgnoreCase(mode)) {
            KafkaToFileService kafkaToFileService = new KafkaToFileService();
            kafkaToFileService.execute(topicClassMap, topicOutputMap, baseDirectory, businessDate);
        } else if ("file_to_kafka".equalsIgnoreCase(mode)) {
            FileToKafkaService fileToKafkaService = new FileToKafkaService();
            fileToKafkaService.execute(topicClassMap, topicOutputMap, baseDirectory, businessDate);
        } else {
            System.out.println("Invalid mode specified in configuration.");
        }
    }
}


Kafkatofileservice full

package com.example.service;

import com.example.util.FixmlObjectProcessor;
import com.google.protobuf.Message;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import java.io.*;
import java.lang.reflect.Method;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

public class KafkaToFileService {

    public void execute(Map<String, String> topicClassMap, Map<String, String> topicOutputMap,
                        String baseDirectory, String businessDate) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        Path outputDir = Paths.get(baseDirectory, timestamp);
        try {
            Files.createDirectories(outputDir);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        ExecutorService executorService = Executors.newFixedThreadPool(topicClassMap.size());

        for (Map.Entry<String, String> entry : topicClassMap.entrySet()) {
            String topic = entry.getKey();
            String className = entry.getValue();
            String outputTopic = topicOutputMap.get(topic);
            executorService.submit(() -> processTopic(topic, className, outputTopic, outputDir, businessDate));
        }

        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void processTopic(String topic, String className, String outputTopic,
                              Path outputDir, String businessDate) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group-" + topic);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topic));
            Path filePath = outputDir.resolve(topic + ".dat");

            try (OutputStream os = Files.newOutputStream(filePath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                while (true) {
                    ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
                    if (records.isEmpty()) break;

                    for (ConsumerRecord<String, byte[]> record : records) {
                        byte[] messageBytes = record.value();
                        os.write(intToBytes(messageBytes.length));
                        os.write(messageBytes);
                    }
                }
            }

            // After writing to file, read, update, and produce to Kafka
            byte[] fileContent = Files.readAllBytes(filePath);
            List<byte[]> messages = extractMessages(fileContent);

            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

            try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {
                for (byte[] messageBytes : messages) {
                    Message message = deserializeMessage(messageBytes, className);
                    Message updatedMessage = updateMessage(message, businessDate);
                    byte[] updatedBytes = updatedMessage.toByteArray();
                    ProducerRecord<String, byte[]> record = new ProducerRecord<>(outputTopic, updatedBytes);
                    producer.send(record);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<byte[]> extractMessages(byte[] fileContent) throws IOException {
        List<byte[]> messages = new ArrayList<>();
        ByteArrayInputStream bais = new ByteArrayInputStream(fileContent);
        DataInputStream dis = new DataInputStream(bais);

        while (dis.available() > 0) {
            int length = dis.readInt();
            byte[] messageBytes = new byte[length];
            dis.readFully(messageBytes);
            messages.add(messageBytes);
        }

        return messages;
    }

    private Message deserializeMessage(byte[] messageBytes, String className) throws Exception {
        Class<?> clazz = Class.forName(className);
        Method parseFrom = clazz.getMethod("parseFrom", byte[].class);
        return (Message) parseFrom.invoke(null, messageBytes);
    }

    private Message updateMessage(Message message, String newBusinessDate) throws Exception {
        // Assuming the message has a getPayload() method returning FixmlObject
        Method getPayload = message.getClass().getMethod("getPayload");
        Object payload = getPayload.invoke(message);

        // Update businessDate
        Method setBusinessDate = payload.getClass().getMethod("setBusinessDate", String.class);
        setBusinessDate.invoke(payload, newBusinessDate);

        // Update fixmlPayload
        Method getFixmlPayload = payload.getClass().getMethod("getFixmlPayload");
        String fixmlPayload = (String) getFixmlPayload.invoke(payload);
        String updatedFixmlPayload = FixmlObjectProcessor.updateBizDateInXml(fixmlPayload, newBusinessDate);
        Method setFixmlPayload = payload.getClass().getMethod("setFixmlPayload", String.class);
        setFixmlPayload.invoke(payload, updatedFixmlPayload);

        // Set the updated payload back to the message
        Method setPayload = message.getClass().getMethod("setPayload", payload.getClass());
        setPayload.invoke(message, payload);

        return message;
    }

    private byte[] intToBytes(int value) {
        return new byte[] {
            (byte)(value >>> 24),
            (byte)(value >>> 16),
            (byte)(value >>> 8),
            (byte)value
        };
    }
}

Filetokafkaservice

package com.example.service;

import com.example.util.FixmlObjectProcessor;
import com.google.protobuf.Message;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class FileToKafkaService {

    public void execute(Map<String, String> topicClassMap, Map<String, String> topicOutputMap,
                        String baseDirectory, String businessDate) {
        Path latestDir = getLatestTimestampedDirectory(baseDirectory);
        if (latestDir == null) {
            System.out.println("No timestamped directories found in base directory.");
            return;
        }

        for (Map.Entry<String, String> entry : topicClassMap.entrySet()) {
            String topic = entry.getKey();
            String className = entry.getValue();
            String outputTopic = topicOutputMap.get(topic);
            Path filePath = latestDir.resolve(topic + ".dat");
            if (!Files.exists(filePath)) {
                System.out.println("File not found for topic: " + topic);
                continue;
            }
            processFile(filePath, className, outputTopic, businessDate);
        }
    }

    private Path getLatestTimestampedDirectory(String baseDirectory) {
        try (Stream<Path> paths = Files.list(Paths.get(baseDirectory))) {
            return paths.filter(Files::isDirectory)
                    .max(Comparator.comparing(Path::getFileName))
                    .orElse(null);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void processFile(Path filePath, String className, String outputTopic, String businessDate) {
        try {
            byte[] fileContent = Files.readAllBytes(filePath);
            List<byte[]> messages = extractMessages(fileContent);

            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());

            try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {
                for (byte[] messageBytes : messages) {
                    Message message = deserializeMessage(messageBytes, className);
                    Message updatedMessage = updateMessage(message, businessDate);
                    byte[] updatedBytes = updatedMessage.toByteArray();
                    ProducerRecord<String, byte[]> record = new ProducerRecord<>(outputTopic, updatedBytes);
                    producer.send(record);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<byte[]> extractMessages(byte[] fileContent) throws IOException {
        List<byte[]> messages = new ArrayList<>();
        ByteArrayInputStream bais = new ByteArrayInputStream(fileContent);
        DataInputStream dis = new DataInputStream(bais);

        while (dis.available() > 0) {
            int length = dis.readInt();
            byte[] messageBytes = new byte[length];
            dis.readFully(messageBytes);
            messages.add(messageBytes);
        }

        return messages;
    }

    private Message deserializeMessage(byte[] messageBytes, String className) throws Exception {
        Class<?> clazz = Class.forName(className);
        Method parseFrom = clazz.getMethod("parseFrom", byte[].class);
        return (Message) parseFrom.invoke(null, messageBytes);
    }

    private Message updateMessage(Message message, String newBusinessDate) throws Exception {
        Method getPayload = message.getClass().getMethod("getPayload");
        Object payload = getPayload.invoke(message);

        Method getFixmlPayload = payload.getClass().getMethod("getFixmlPayload");
        String fixmlPayload = (String) getFixmlPayload.invoke(payload);
        String updatedFixmlPayload = FixmlObjectProcessor.updateBizDateInXml(fixmlPayload, newBusinessDate);

        Method setBusinessDate = payload.getClass().getMethod("setBusinessDate", String.class);
        setBusinessDate.invoke(payload, newBusinessDate);

        Method setFixmlPayload = payload.getClass().getMethod("setFixmlPayload", String.class);
        setFixmlPayload.invoke(payload, updatedFixmlPayload);

        Method setPayload = message.getClass().getMethod("setPayload", payload.getClass());
        setPayload.invoke(message, payload);

        return message;
    }
}
        
