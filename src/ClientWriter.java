import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.*;

public class ClientWriter {
    private final static String EXCHANGE_NAME = "fanout_exchange";
    private static int messageId = 1;  // Variable pour l'ID du message
    private static final String MESSAGE_ID_FILE = "messageId.txt";


    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: java ClientWriter <message>");
            return;
        }

        String messageContent = args[0];

        // Appel de la nouvelle méthode pour envoyer le message
        sendMessage(messageContent);
    }

    private static void sendMessage(String messageContent) throws Exception {
        // Charger la dernière valeur de messageId depuis le fichier
        loadMessageId();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // Déclaration de l'échange en mode fanout
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

            // Création du message composé de l'ID et du contenu
            String message = messageId + " " + messageContent;

            // Publication du message à l'échange fanout
            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());

            System.out.println("Sent message: '" + message + "' to exchange '" + EXCHANGE_NAME + "'");

            // Incrémenter l'ID du message pour le prochain message
            messageId++;

            // Sauvegarder la nouvelle valeur de messageId dans le fichier
            saveMessageId();
        }
    }

    private static void loadMessageId() {
        try (BufferedReader reader = new BufferedReader(new FileReader(MESSAGE_ID_FILE))) {
            String line = reader.readLine();
            if (line != null && !line.isEmpty()) {
                messageId = Integer.parseInt(line);
            }
        } catch (IOException | NumberFormatException e) {
            System.out.println("Error loading messageId from file: " + e.getMessage());
        }
    }

    private static void saveMessageId() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(MESSAGE_ID_FILE))) {
            writer.write(String.valueOf(messageId));
        } catch (IOException e) {
            System.out.println("Error saving messageId to file: " + e.getMessage());
        }
    }
}
