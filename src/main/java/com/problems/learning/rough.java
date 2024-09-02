import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;

import java.util.Properties;

public class KafkaProducerWithRetry {

    private static final int MAX_RETRIES = 5;
    private static final long RETRY_INTERVAL_MS = 1000; // 1 second

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String topic = "my-topic";
        String key = "my-key";
        String value = "my-message";

        sendMessageWithRetry(producer, topic, key, value, 0);
    }

    public static void sendMessageWithRetry(KafkaProducer<String, String> producer, String topic, String key, String value, int attempt) {
        producer.send(new ProducerRecord<>(topic, key, value), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    // Message sent successfully
                    System.out.println("Message sent successfully: " + metadata.toString());
                } else {
                    // Message failed to send
                    if (exception instanceof RetriableException || exception instanceof TimeoutException) {
                        // Retry logic for retriable exceptions
                        if (attempt < MAX_RETRIES) {
                            System.out.println("Retrying to send message, attempt " + (attempt + 1));
                            try {
                                Thread.sleep(RETRY_INTERVAL_MS);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                                Thread.currentThread().interrupt();
                            }
                            sendMessageWithRetry(producer, topic, key, value, attempt + 1);
                        } else {
                            System.err.println("Failed to send message after " + MAX_RETRIES + " attempts");
                        }
                    } else {
                        // Non-retriable exception, log the error
                        System.err.println("Failed to send message due to non-retriable exception: " + exception.getMessage());
                    }
                }
            }
        });
    }
}
