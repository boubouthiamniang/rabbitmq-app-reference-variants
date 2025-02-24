package com.messaging.rabbitmq.productVariants.fanoutExchange;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

public class Publisher {
  
  private static final String EXCHANGE_NAME = "exchange.xp";
  private static final String ROUTING_KEY = "routingKeyXp";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    try (Connection connection = factory.newConnection();
         Channel channel = connection.createChannel()) {
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

        String message = "Thank you mario, but our princess is in another castle";

        channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, null, message.getBytes("UTF-8"));
    }
  }
}
