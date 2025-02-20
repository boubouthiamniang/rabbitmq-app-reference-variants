package com.messaging.rabbitmq.productVariants.RPC;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class RPCServer {
    private static final String RPC_REQUEST_QUEUE_NAME = "queue.rpc.reply";

    public static void main(String[] argv) throws Exception {
        
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection(); 
             Channel channel = connection.createChannel()) {
            
            // Declare the queue
            channel.queueDeclare(RPC_REQUEST_QUEUE_NAME, false, false, false, null);
            channel.queuePurge(RPC_REQUEST_QUEUE_NAME);
            channel.basicQos(1);  // Limit to 1 message at a time per consumer

            // Create a callback to process incoming requests
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String response = "";
                try {
                    String message = new String(delivery.getBody(), "UTF-8");
                    System.out.println(" [.] Processing " + message);
                    response = processRequest(message);
                } catch (RuntimeException e) {
                    System.out.println(" [.] Error: " + e.toString());
                } finally {
                    // Send the response back to the client
                    String replyTo = delivery.getProperties().getReplyTo();
                    AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                            .Builder()
                            .correlationId(delivery.getProperties().getCorrelationId())
                            .build();
                    channel.basicPublish("", replyTo, replyProps, response.getBytes("UTF-8"));
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            };

            // Set up the consumer
            channel.basicConsume(RPC_REQUEST_QUEUE_NAME, false, deliverCallback, consumerTag -> { });
        }
    }

    // A simple function to simulate processing
    private static String processRequest(String message) {
        return "Processed: " + message;
    }
}
