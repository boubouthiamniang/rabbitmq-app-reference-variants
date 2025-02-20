package com.messaging.rabbitmq.productVariants.RPC;



import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class RPCClient {
    private static final String RPC_REQUEST_QUEUE_NAME = "queue.rpc.request";
    private static final String RPC_REPLY_QUEUE_NAME = "queue.rpc.reply";
    private static final String RPC_REQUEST_EXCHANGE_NAME = "";

    public static void main(String[] argv) throws Exception {
        // Set up a connection to RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()) {

            channel.queueDeclare(RPC_REPLY_QUEUE_NAME, true, false, false, null);

            String correlationId = UUID.randomUUID().toString();

            // Create properties with correlation ID and reply queue
            AMQP.BasicProperties props = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(correlationId)
                    .replyTo(RPC_REPLY_QUEUE_NAME)
                    .build();

            String message = "myRequest";
            channel.basicPublish(RPC_REQUEST_EXCHANGE_NAME, RPC_REQUEST_QUEUE_NAME, props, message.getBytes(StandardCharsets.UTF_8));

            // Use a BlockingQueue to wait for the response
            final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                if (delivery.getProperties().getCorrelationId().equals(correlationId)) {
                    response.offer(new String(delivery.getBody(), StandardCharsets.UTF_8));
                }
            };

            // Consume messages from the reply queue
            channel.basicConsume(RPC_REPLY_QUEUE_NAME, true, deliverCallback, consumerTag -> { });
        }
    }
}
