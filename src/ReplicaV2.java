import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;

public class ReplicaV2 {
    private static final String EXCHANGE_NAME = "read_all_exchange";
    private static final String RESPONSE_QUEUE = "response_queue";
    private static final int NUM_REPLICAS = 3;

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java ReplicaV2 <replica_number>");
            System.exit(1);
        }

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            String responseQueueName = channel.queueDeclare().getQueue();
            channel.queueBind(responseQueueName, EXCHANGE_NAME, "");

            System.out.println(" [*] ReplicaV2  waiting for requests.");

            // Define callback for handling incoming requests
            DeliverCallback requestCallback = (consumerTag, delivery) -> {
                String requestMessage = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.println(" [x] Received request: " + requestMessage);

                List<String> responseLines = processRequest();
                for (String line : responseLines) {
                    channel.basicPublish("", delivery.getProperties().getReplyTo(), null, line.getBytes(StandardCharsets.UTF_8));
                }
            };

            // Consume messages from the response queue
            channel.basicConsume(responseQueueName, true, requestCallback, consumerTag -> {
            });

            // Keep the main thread running to wait for requests
            while (true) {
                try {
                    Thread.sleep(1000); // Sleep for a short duration to avoid busy waiting
                } catch (InterruptedException e) {
                    System.err.println(" [!] Thread interrupted: " + e.getMessage());
                    Thread.currentThread().interrupt(); // Restore interrupted status
                }
            }

        } catch (IOException | TimeoutException e) {
            System.err.println(" [!] Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Method to process the request
    private static List<String> processRequest() {
        List<BufferedReader> readers = new ArrayList<>();
        List<String> responseLines = new ArrayList<>();

        try {
            // Open readers for each replica file
            for (int i = 1; i <= NUM_REPLICAS; i++) {
                String fileName = "replica_" + i + ".txt";
                readers.add(new BufferedReader(new FileReader(fileName)));
            }

            // Read lines from each file and update line counts
            boolean allFilesNotEmpty = true;
            while (allFilesNotEmpty) {
                allFilesNotEmpty = false;
                Map<String, Integer> lineCounts = new HashMap<>();

                // Read a line from each file
                for (BufferedReader reader : readers) {
                    String line = reader.readLine();
                    if (line != null) {
                        allFilesNotEmpty = true;
                        lineCounts.put(line, lineCounts.getOrDefault(line, 0) + 1);
                    }
                }

                // Find the line with the maximum occurrences
                int maxOccurrences = 0;
                String maxLine = null;
                for (Map.Entry<String, Integer> entry : lineCounts.entrySet()) {
                    if (entry.getValue() > maxOccurrences) {
                        maxOccurrences = entry.getValue();
                        maxLine = entry.getKey();
                    }
                }

                if (maxLine != null) {
                    responseLines.add(maxLine);
                }
            }
        } catch (IOException e) {
            System.err.println(" [!] Error reading files: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Close all readers
            for (BufferedReader reader : readers) {
                try {
                    if (reader != null) reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return responseLines;
    }
}
