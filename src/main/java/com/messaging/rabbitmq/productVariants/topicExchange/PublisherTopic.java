package com.messaging.rabbitmq.productVariants.topicExchange;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class PublisherTopic {

    private static final String EXCHANGE_NAME = "exchage.*";
    private static final String ROUTING_KEY = "routingKeyXp";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()) {

            channel.exchangeDeclare(EXCHANGE_NAME, "topic");

            String message = "Thank you Mario, but our princess is in another castle";

            channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + ROUTING_KEY + "':'" + message + "'");
        }
    }
    
}
