import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientReaderV2 {
    private final static String EXCHANGE_NAME = "read_all_exchange";
    private static final int NUM_REPLICAS = 3;
    private static final int MINIMUM_OCCURRENCES = 2; // Lignes doivent apparaître dans au moins 2 répliques

    public static void main(String[] args) throws Exception {
        new ClientReaderV2().run();
    }

    public void run() throws Exception {
        String requestMessage = "Read All";
        AtomicInteger responseCounter = new AtomicInteger(0); // Compteur pour suivre le nombre de réponses reçues

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // Déclaration de l'échange en mode fanout
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

            // Création d'une queue temporaire pour recevoir les réponses des Replica
            String replyQueueName = channel.queueDeclare().getQueue();
            channel.queueBind(replyQueueName, EXCHANGE_NAME, "");

            // Publication de la requête à l'échange fanout
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .replyTo(replyQueueName)
                    .build();

            channel.basicPublish(EXCHANGE_NAME, "", props, requestMessage.getBytes(StandardCharsets.UTF_8));
            System.out.println("Sent request message: '" + requestMessage + "' to exchange '" + EXCHANGE_NAME + "'");

            // Stockage des lignes de chaque réplique
            List<List<String>>  replicaLines = new ArrayList<>(NUM_REPLICAS);
            for (int i = 1; i <= NUM_REPLICAS; i++) {
                replicaLines.add(new ArrayList<>());
            }

            // Réception des réponses des Replica
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String replyMessage = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.println("Received reply message: '" + replyMessage + "'");
                String[] lines = replyMessage.split("\n");
                for (int i = 0; i < lines.length; i++) {
                    replicaLines.get(i).add(lines[i]);
                }
                responseCounter.incrementAndGet(); // Incrémenter le compteur de réponses
            };

            channel.basicConsume(replyQueueName, true, deliverCallback, consumerTag -> {
            });

            // Attendre jusqu'à recevoir toutes les réponses attendues
            while (responseCounter.get() < NUM_REPLICAS) {
                Thread.sleep(1000); // Attendre 1 seconde avant de vérifier à nouveau
            }

            // Trouver les lignes qui apparaissent dans la majorité des propositions des 3 répliques
            Map<String, Integer> lineOccurrences = new HashMap<>();
            for (List<String> lines : replicaLines) {
                for (String line : lines) {
                    lineOccurrences.put(line, lineOccurrences.getOrDefault(line, 0) + 1);
                }
            }


        }
    }
}
