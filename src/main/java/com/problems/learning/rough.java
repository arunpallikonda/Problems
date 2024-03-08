import io.reactivex.rxjava3.core.Observable;
import java.sql.*;

public class Db2PaginatedReader {

    private static Observable<List<MyData>> createPaginatedDb2Observable(String dbUrl, String user, String password, final int pageSize) {
        return Observable.create(emitter -> {
            try (Connection conn = DriverManager.getConnection(dbUrl, user, password)) {
                int pageNumber = 0;
                while (!emitter.isDisposed()) {
                    List<MyData> pageData = new ArrayList<>();
                    String query = "SELECT * FROM YourTable " +
                                   "ORDER BY YourOrderColumn " + // Make sure to order by a column
                                   "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";
                    try (PreparedStatement stmt = conn.prepareStatement(query)) {
                        stmt.setInt(1, pageNumber * pageSize);
                        stmt.setInt(2, pageSize);
                        ResultSet rs = stmt.executeQuery();
                        boolean hasData = false;
                        while (rs.next()) {
                            hasData = true;
                            // Assuming MyData can be built from ResultSet
                            MyData data = MyData.newBuilder()
                                                .setKey(rs.getString("key"))
                                                // Set other fields
                                                .build();
                            pageData.add(data);
                        }
                        if (hasData) {
                            emitter.onNext(pageData);
                            pageNumber++;
                        } else {
                            emitter.onComplete();
                            break;
                        }
                    }
                }
            } catch (SQLException e) {
                emitter.onError(e);
            }
        }).subscribeOn(Schedulers.io());
    }
}



public static void main(String[] args) {
    String dbUrl = "jdbc:db2://YourDb2Url";
    String dbUser = "user";
    String dbPassword = "password";
    final int pageSize = 10000; // Adjust based on your performance needs

    Observable<List<MyData>> db2Observable = createPaginatedDb2Observable(dbUrl, dbUser, dbPassword, pageSize);

    db2Observable.subscribe(pageData -> {
        // Process each page of data
        for (MyData data : pageData) {
            // Process data
        }
    }, Throwable::printStackTrace);
}









==============

  import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.*;
import java.util.AbstractMap;
import java.util.Properties;

public class DataProcessorApp {

    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        // Initialize Kafka properties
        kafkaProps.put("bootstrap.servers", "your.kafka.server:9092");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        kafkaProps.put("group.id", "your-consumer-group");

        String dbUrl = "jdbc:db2://YourDb2Url";
        String dbUser = "user";
        String dbPassword = "password";

        Observable<ConsumerRecord<String, byte[]>> kafkaObservable = KafkaParallelConsumer.createKafkaObservable("YourTopicName", kafkaProps);
        Observable<MyData> db2Observable = createDb2Observable(dbUrl, dbUser, dbPassword);

        Observable.zip(kafkaObservable.map(record -> MyData.parseFrom(record.value())), 
                       db2Observable, 
                       (kafkaData, db2Data) -> new AbstractMap.SimpleEntry<>(kafkaData, db2Data))
                  .subscribeOn(Schedulers.io())
                  .subscribe(entry -> {
                      // Compare kafkaData and db2Data, then process accordingly
                      MyData kafkaData = entry.getKey();
                      MyData db2Data = entry.getValue();
                      if (kafkaData.equals(db2Data)) {
                          // Write to success file
                      } else {
                          // Write to failure file
                      }
                  }, error -> error.printStackTrace());
    }

    private static Observable<MyData> createDb2Observable(String dbUrl, String user, String password) {
        return Observable.create(emitter -> {
            try (Connection conn = DriverManager.getConnection(dbUrl, user, password);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM YourTable")) {
                
                while (rs.next() && !emitter.isDisposed()) {
                    MyData data = MyData.newBuilder()
                                        .setKey(rs.getString("key"))
                                        // Set other fields from the result set
                                        .build();
                    emitter.onNext(data);
                }
            } catch (SQLException e) {
                emitter.onError(e);
            }
            emitter.onComplete();
        }).subscribeOn(Schedulers.io());
    }
}



import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class KafkaParallelConsumer {

    private static List<Observable<ConsumerRecord<String, byte[]>>> createPartitionObservables(String topicName, Properties props) {
        // Temporary consumer to get partition info
        KafkaConsumer<String, byte[]> tempConsumer = new KafkaConsumer<>(props);
        tempConsumer.subscribe(Arrays.asList(topicName));
        List<PartitionInfo> partitions = tempConsumer.partitionsFor(topicName);
        tempConsumer.close();

        // Create an observable for each partition
        return partitions.stream().map(partitionInfo -> {
            TopicPartition topicPartition = new TopicPartition(topicName, partitionInfo.partition());
            return Observable.<ConsumerRecord<String, byte[]>>create(emitter -> {
                KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
                consumer.assign(Collections.singletonList(topicPartition));

                while (!emitter.isDisposed()) {
                    consumer.poll(Duration.ofMillis(100)).forEach(emitter::onNext);
                }
                consumer.close();
            }).subscribeOn(Schedulers.from(Executors.newSingleThreadExecutor())); // Use a single thread executor for each partition
        }).collect(Collectors.toList());
    }

    public static Observable<ConsumerRecord<String, byte[]>> createKafkaObservable(String topicName, Properties props) {
        List<Observable<ConsumerRecord<String, byte[]>>> partitionObservables = createPartitionObservables(topicName, props);
        // Merge all partition observables into one observable
        return Observable.merge(partitionObservables);
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "your.kafka.server:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "your-consumer-group");

        Observable<ConsumerRecord<String, byte[]>> kafkaObservable = createKafkaObservable("YourTopicName", props);

        kafkaObservable.subscribe(record -> {
            // Process each record
            System.out.println("Received: " + record);
        }, Throwable::printStackTrace);
    }
}













------------


    import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaDBComparisonApp {

    public static void main(String[] args) {
        String kafkaTopic = "your_kafka_topic";
        String db2Query = "SELECT * FROM your_table";
        String keyField = "your_key_field";

        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "your_kafka_bootstrap_servers");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group");
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "your_kafka_bootstrap_servers");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Kafka consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaProps);
        kafkaConsumer.subscribe(Collections.singletonList(kafkaTopic));

        // Create Kafka producer for success and failure outputs
        KafkaProducer<String, String> successProducer = new KafkaProducer<>(producerProps);
        KafkaProducer<String, String> failureProducer = new KafkaProducer<>(producerProps);

        // Create a DB2 pagination observable
        AtomicInteger currentPage = new AtomicInteger(0);
        int pageSize = 1000; // Set your preferred page size
        Observable<List<ProtoObject>> db2Observable = Observable.create(emitter -> {
            boolean hasNextPage = true;
            while (hasNextPage) {
                List<ProtoObject> db2Results = fetchDataFromDB2(db2Query, currentPage.getAndIncrement(), pageSize);
                if (db2Results.isEmpty()) {
                    hasNextPage = false;
                } else {
                    emitter.onNext(db2Results);
                }
            }
            emitter.onComplete();
        }).subscribeOn(Schedulers.io());

        // Create a Kafka observable for ProtoObject deserialization
        Observable<ProtoObject> kafkaObservable = Observable.create(emitter -> {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    // Deserialize ByteBuffer into ProtoObject
                    ProtoObject protoObject = deserializeProtoObject(record.value());
                    emitter.onNext(protoObject);
                }
            }
        }).subscribeOn(Schedulers.io());

        // Combine the two observables based on the key field
        Observable<Pair<ProtoObject, List<ProtoObject>>> combinedObservable = Observable.combineLatest(
            kafkaObservable.groupBy(ProtoObject::getKey),
            db2Observable.groupBy(ProtoObject::getKey),
            (kafkaGroup, db2Group) -> {
                Pair<ProtoObject, List<ProtoObject>> pair = new Pair<>();
                kafkaGroup.subscribe(pair::setFirst);
                db2Group.subscribe(pair::setSecond);
                return pair;
            }
        );

        // Process and compare ProtoObjects, then write to success or failure Kafka topic
        combinedObservable.subscribe(pair -> {
            ProtoObject kafkaProto = pair.getFirst();
            List<ProtoObject> db2Protos = pair.getSecond();

            boolean isMatch = false;

            for (ProtoObject db2Proto : db2Protos) {
                if (compareProtoObjects(kafkaProto, db2Proto)) {
                    isMatch = true;
                    break;
                }
            }

            if (isMatch) {
                // Write to success Kafka topic
                successProducer.send(new ProducerRecord<>("success_topic", kafkaProto.getKey(), kafkaProto.toString()));
            } else {
                // Write to failure Kafka topic
                failureProducer.send(new ProducerRecord<>("failure_topic", kafkaProto.getKey(), kafkaProto.toString()));
            }
        });
    }

    // Implement this method to fetch data from DB2 using pagination
    private static List<ProtoObject> fetchDataFromDB2(String query, int page, int pageSize) {
        // Implement database query and pagination logic
        // Return a list of ProtoObjects for the given page
        return Collections.emptyList(); // Placeholder
    }

    // Implement this method to deserialize ByteBuffer into ProtoObject
    private static ProtoObject deserializeProtoObject(String serializedData) {
        // Deserialize the data and return a ProtoObject
        return new ProtoObject(); // Placeholder
    }

    // Implement this method to compare two ProtoObjects
    private static boolean compareProtoObjects(ProtoObject proto1, ProtoObject proto2) {
        // Implement comparison logic
        return false; // Placeholder
    }

    static class Pair<A, B> {
        private A first;
        private B second;

        public A getFirst() {
            return first;
        }

        public void setFirst(A first) {
            this.first = first;
        }

        public B getSecond() {
            return second;
        }

        public void setSecond(B second) {
            this.second = second;
        }
    }
}








import os
import requests
import zipfile
import json

def download_and_call_api(nexus_url, api_url):
    # Download the zip file from Nexus
    response = requests.get(nexus_url)
    if response.status_code != 200:
        print("Failed to download the zip file from Nexus.")
        return

    # Create a temporary directory to extract the zip file
    temp_dir = "temp_zip_extraction"
    os.makedirs(temp_dir, exist_ok=True)

    # Save the downloaded zip file
    zip_file_path = os.path.join(temp_dir, "downloaded_zip.zip")
    with open(zip_file_path, "wb") as f:
        f.write(response.content)

    # Extract the zip file
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(temp_dir)

    # Read the JSON file from the extracted zip
    json_file_path = os.path.join(temp_dir, "*.json")
    json_files = [f for f in os.listdir(temp_dir) if f.endswith('.json')]
    if len(json_files) != 1:
        print("Expected one JSON file in the zip, but found", len(json_files))
        return
    json_file_path = os.path.join(temp_dir, json_files[0])

    # Load the JSON data
    with open(json_file_path) as json_file:
        payload = json.load(json_file)

    # Call the API with the payload
    api_response = requests.post(api_url, json=payload)
    if api_response.status_code == 200:
        print("API call successful.")
        print("Response:", api_response.json())
    else:
        print("API call failed with status code:", api_response.status_code)

    # Clean up temporary files
    os.remove(zip_file_path)
    os.remove(json_file_path)
    os.rmdir(temp_dir)

# Example usage:
nexus_url = "https://example.com/path/to/your/zip/file.zip"
api_url = "https://api.example.com/endpoint"
download_and_call_api(nexus_url, api_url)

