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

