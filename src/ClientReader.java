import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;

public class ClientReader {
    private final static String EXCHANGE_NAME = "fanout_exchange";

    public static void main(String[] args) throws Exception {
        String requestMessage = "Read Last";

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
            };

            channel.basicConsume(replyQueueName, true, deliverCallback, consumerTag -> { });

            System.out.println("Waiting for reply messages from Replica...");
            Thread.sleep(5000); // Attendre 5 secondes pour recevoir les réponses
        }
    }
}
