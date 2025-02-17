package com.messaging.rabbitmq.productVariants.PublisherConfirms;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.time.Duration;

public class PublisherConfirms {

    static final int MESSAGE_COUNT = 50000;

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        try (Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()) {

            String queue = "queueXp";
            channel.queueDeclare(queue, false, false, true, null);

            String message = "Thank you mario, but our princess is in another castle";

            channel.confirmSelect();
            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                channel.basicPublish("", queue, null, message.getBytes());
                channel.waitForConfirmsOrDie(5000);
            }
            long end = System.nanoTime();
            System.out.format("Published %,d messages individually in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }
    }
}
