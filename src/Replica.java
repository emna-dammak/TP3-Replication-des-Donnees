import com.rabbitmq.client.*;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class Replica {
    private final static String EXCHANGE_NAME = "fanout_exchange";

    public static void main(String[] args) {
        // Appel de la méthode pour démarrer la réplique
        startReplica(args);
    }

    private static void startReplica(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java Replica <process_number>");
            return;
        }

        int processNumber = Integer.parseInt(args[0]);


        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {


            // Déclaration de la queue spécifique au processus
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, EXCHANGE_NAME, "");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println("Received message: '" + message + "'");

                if ("Read Last".equals(message)) {
                    String lastLine = readLastLineFromFile(processNumber);
                    if (lastLine != null) {
                        // Envoyer la dernière ligne comme réponse
                        channel.basicPublish("", delivery.getProperties().getReplyTo(), null, lastLine.getBytes(StandardCharsets.UTF_8));
                        System.out.println("Sent response message: '" + lastLine + "'");
                    }
                } else {
                    // Ajouter la ligne au fichier local
                    appendToFile(processNumber, message);
                }
            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });

            System.out.println("Waiting for messages. Process Number: " + processNumber);
            while (true) {
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void appendToFile(int processNumber, String message) {
        String fileName = "replica_" + processNumber + ".txt";
        try (FileWriter writer = new FileWriter(fileName, true)) {
            writer.write(message + "\n");
            System.out.println("Added line to " + fileName + ": " + message);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static String readLastLineFromFile(int processNumber) {
        String fileName = "replica_" + processNumber + ".txt";
        String lastLine = null;

        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String currentLine;
            while ((currentLine = br.readLine()) != null) {
                lastLine = currentLine;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return lastLine;
    }
}

