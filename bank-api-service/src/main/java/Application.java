import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Distributed Systems
 * Banking API Service
 * Assessment 2
 *
 * @author Grace Keane
 * @version Java15
 */
public class Application {
    // Assigning constant topics
    private static final String VALID_TRANSACTIONS_TOPIC = "valid-transactions";
    private static final String SUSPICIOUS_TRANSACTIONS_TOPIC = "suspicious-transactions";
    private static final String REPORTING_SERVICE_TOPIC = "valid-transactions" + "suspicious-transactions";
    // Assigning address of three brokers
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {
        IncomingTransactionsReader incomingTransactionsReader = new IncomingTransactionsReader();
        CustomerAddressDatabase customerAddressDatabase = new CustomerAddressDatabase();

        Application kafkaApplication = new Application();
        Producer<String, Transaction> kafkaProducer = kafkaApplication.createKafkaProducer(BOOTSTRAP_SERVERS);

        try {
            kafkaApplication.processTransactions(incomingTransactionsReader, customerAddressDatabase, kafkaProducer);
        } catch (ExecutionException | InterruptedException e){
            e.printStackTrace();
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }

    /*
    * Method to produce kafka messages and retrieve the next transaction from the IncomingTransactionReader.
    * Then get user residence from the UserResidenceDatabase to compare user residence to transaction
    * location. After, sends a message to the appropriate topic, depending on whether the user
    * residence and transaction location match.
    *
    * @param incomingTransactionReader file to read customer transactions
    * @param customerAddressDatabase text file that stores the customers address info
    * @param kafkaProducer passing in kafkaProducer method to generate messages
    *
    * @throws ExecutionException
    * @throws InterruptedException
    * */
    public void processTransactions(IncomingTransactionsReader incomingTransactionsReader, CustomerAddressDatabase customerAddressDatabase,
                                    Producer<String, Transaction> kafkaProducer) throws ExecutionException, InterruptedException {

            int transactionCount = 20;

            // Iterates through all the file
            for (int i = 0; i < transactionCount; i++) {

                if (incomingTransactionsReader.hasNext()) {
                    Transaction transaction = incomingTransactionsReader.next();
                    transactionCount++;

                    long timestamp = System.currentTimeMillis();

                    // Send information to VALID_TRANSACTIONS_TOPIC if user residence and transaction location match
                    if (customerAddressDatabase.getUserResidence(transaction.getUser()).equals(transaction.getTransactionLocation())) {
                        //  Creating a new producer record
                        ProducerRecord<String, Transaction> record = new ProducerRecord<>(VALID_TRANSACTIONS_TOPIC,
                                transaction.getUser(), transaction);
                        // Sending data to kafka & getting sent back info from recordMetadata to specify where info went
                        RecordMetadata recordMetadata = kafkaProducer.send(record).get();
                        System.out.println(String.format("Record with (key %s, value: %s) was sent to (partition %d, offset: %d, topic: %s)",
                                record.key(), record.value(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.topic()));

                    } else {
                        // Send information to SUSPICIOUS_TRANSACTION_TOPIC if user residence and transaction location do not match
                        ProducerRecord<String, Transaction> record = new ProducerRecord<>(SUSPICIOUS_TRANSACTIONS_TOPIC,
                                transaction.getUser(), transaction);

                        RecordMetadata recordMetadata = kafkaProducer.send(record).get();
                        System.out.println(String.format("Record with (key %s, value: %s) was sent to (partition %d, offset: %d, topic: %s)",
                                record.key(), record.value(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.topic()));

                    }

                    // Attempted to send both topics to report-service
                    ProducerRecord<String, Transaction> record = new ProducerRecord<>(REPORTING_SERVICE_TOPIC,
                            transaction.getUser(), transaction);

                    RecordMetadata recordMetadata = kafkaProducer.send(record).get();
                    System.out.println(String.format("Record with (key %s, value: %s) was sent to (partition %d, offset: %d, topic: %s)",
                            record.key(), record.value(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.topic()));

                }
            }
    }

    /*
    * Creating a producer with main properties needed.
    *
    * @param bootstrapServers specifies how to find the kafka cluster.
    */
    public Producer<String, Transaction> createKafkaProducer(String bootstrapServers) {
        // Configure producer
        Properties properties = new Properties();

        // Creating key value pairs and put into properties object
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers); // Identify location of server
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "bank-producer"); // Giving producer a name
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // Configuring for key serializer value
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Transaction.TransactionSerializer.class.getName()); // Adding a transaction serializer

        KafkaProducer kafkaProducer = new KafkaProducer(properties);
        System.out.println("bank producer");
        // Return a new KafkaProducer and pass in properties object just created
        return new KafkaProducer<String, Transaction>(properties);
    }
}
