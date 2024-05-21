import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientReader {
    private final static String EXCHANGE_NAME = "fanout_exchange";
    private static final int EXPECTED_REPLICA_COUNT = 4; // Nombre attendu de réponses des répliques

    public static void main(String[] args) throws Exception {
        new ClientReader().run();
    }

    public void run() throws Exception {
        String requestMessage = "Read Last";
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

            // Réception des réponses des Replica
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String replyMessage = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.println("Received reply message: '" + replyMessage + "'");
                responseCounter.incrementAndGet(); // Incrémenter le compteur de réponses
            };

            channel.basicConsume(replyQueueName, true, deliverCallback, consumerTag -> { });

            // Attendre jusqu'à recevoir toutes les réponses attendues
            int i=0;
            while (responseCounter.get() < EXPECTED_REPLICA_COUNT && i<9) {
                Thread.sleep(1000); // Attendre 1 seconde avant de vérifier à nouveau
                i++;
            }

            if(responseCounter.get()==EXPECTED_REPLICA_COUNT){
                System.out.println("Received all expected reply messages from Replica.");
            } else {
                System.out.println("Responses are missing");
            }
        }
    }
    public void runV2() throws Exception {
        String requestMessage = "Read Last";
        int MAX_RESPONSES=2;

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

            // Réception des deux premières réponses des Replica
            int responseCount = 0;
            while (responseCount < MAX_RESPONSES) {
                GetResponse response = channel.basicGet(replyQueueName, true);
                if (response != null) {
                    String replyMessage = new String(response.getBody(), StandardCharsets.UTF_8);
                    System.out.println("Received reply message  : '" + replyMessage + "'");
                    responseCount++;
                } else {
                    System.out.println("No more reply messages available.");
                    break;
                }
            }
        }
    }


}
