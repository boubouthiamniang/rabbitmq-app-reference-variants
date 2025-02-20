package com.messaging.rabbitmq.productVariants.PublisherConfirms;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.time.Duration;

public class PublisherConfirmsBatch {

    static int MESSAGE_COUNT = 5000;
    
        public static void main(String[] args) throws Exception {
    
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
    
            try (Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()) {
    
                String queue = "queueXp";
                channel.queueDeclare(queue, false, false, true, null);
    
                channel.confirmSelect();
    
                int batchSize = 100;
                int outstandingMessageCount = 0;
    
                String message = "Thank you mario, but our princess is in another castle";
    
                long start = System.nanoTime();
                for (int i = 0; i < 5000; i++) {
                    channel.basicPublish("", queue, null, message.getBytes());
                    outstandingMessageCount++;
    
                    if (outstandingMessageCount == batchSize) {
                        channel.waitForConfirmsOrDie(5000);
                        outstandingMessageCount = 0;
                    }
                }
    
                if (outstandingMessageCount > 0) {
                    channel.waitForConfirmsOrDie(5_000);
                }
                long end = System.nanoTime();
                System.out.format("Published %,d messages in batch in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }
    }
}   
