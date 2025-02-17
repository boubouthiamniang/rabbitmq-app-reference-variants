package com.messaging.rabbitmq.productVariants.CompetingConsumer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class CompetingConsumer {

  private static final String TASK_QUEUE_NAME = "queue.task";
  
    public static void main(String[] argv) throws Exception {
      ConnectionFactory factory = new ConnectionFactory();
      factory.setHost("localhost");
      final Connection connection = factory.newConnection();
      final Channel channel = connection.createChannel();
  
      channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
      System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
  
      channel.basicQos(1);
  
      DeliverCallback deliverCallback = (consumerTag, delivery) -> {
          String message = new String(delivery.getBody(), "UTF-8");
  
          System.out.println(" [x] Received '" + message + "'");
          try {
              doWork(message);
          } finally {
              System.out.println(" [x] Done");
              channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
          }
      };
      channel.basicConsume(TASK_QUEUE_NAME, false, deliverCallback, consumerTag -> { });
    }
  
  
    public static void doWork(String task) {
      for (int i=0 ; i< 10 ; i++) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException _ignored) {
            Thread.currentThread().interrupt();
        }
    }
  }
}