import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Distributed Systems.
 * Banking API Service.
 * Assessment 2.
 *
 * @author Grace Keane.
 * @version Java15.
 */
public class Application {
    // Assigning constant topic
    private static final String REPORTING_SERVICE_TOPIC = "valid-transactions" + "suspicious-transactions";
    // Assigning address of three brokers
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {

        Application kafkaConsumerApp = new Application();
        String consumerGroup = "reporting-service";

        if(args.length == 1){
            consumerGroup = args[0];
        }

        System.out.println("Consumer is part of consumer group " + consumerGroup);
        Consumer<String, Transaction> kafkaConsumer = kafkaConsumerApp.createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);
        kafkaConsumerApp.consumeMessages(Collections.singletonList(REPORTING_SERVICE_TOPIC), kafkaConsumer);
    }

    /*
     * Consuming the messages from kafka.
     *
     * @param topic name of topic consuming messages from.
     * @param kafkaConsumer passing in kafka consumer.
     */
    public static void consumeMessages(List<String> topics, Consumer<String, Transaction> kafkaConsumer) {

        // Subscribing to the topic
        kafkaConsumer.subscribe(topics);

        // Continually consume messages from the topic
        while (true){
            // Kafka message as viewed from the consumer
            ConsumerRecords<String, Transaction> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            if (consumerRecords.isEmpty()){

            }

            for(ConsumerRecord<String, Transaction> record: consumerRecords){
                // Printing out messages to the consumer
                System.out.println(String.format("Recieved record (key: %s, value: %s, partition: %d, offset: %d, topic: %s)",
                        record.key(), record.value(), record.partition(), record.offset(), record.topic()));
            }
            // Tells kafka done processing messages (final confirmation)
            kafkaConsumer.commitAsync();
        }
    }

    /*
     * Creating a consumer with main properties needed.
     *
     * @param bootstrapServers specifies how to find the kafka cluster.
     * @param consumerGroup enables consumer to join any consumer group specified.
     */
    public static Consumer<String, Transaction> createKafkaConsumer(String bootstrapServers, String consumerGroup) {
        // Configuring consumer
        Properties properties = new Properties();
        // Tell consumer how to connect to Kafka cluster (point to bootstrap server)
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Configuring key and value deserializers to turn raw bitstream into usable object
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Transaction.TransactionDeserializer.class.getName());
        // Passing in an identifier for the consumer group
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        // Configuring how consumer is going to notify kafka that it has received and processed messages correctly
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Manually communicating with kafka

        // Returning new kafka consumer that takes in a properties object
        return new KafkaConsumer<String, Transaction>(properties);

    }

    private static void recordTransactionForReporting(String topic, Transaction transaction) {
        // Print transaction information to the console
        // Print a different message depending on whether transaction is suspicious or valid

        System.out.println(transaction);

    }
}
