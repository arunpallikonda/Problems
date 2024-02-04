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
