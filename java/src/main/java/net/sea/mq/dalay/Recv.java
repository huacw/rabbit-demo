package net.sea.mq.dalay;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;

/**
 * @author huacw
 * @date 2019/6/22
 */
public class Recv {
    public static void main(String[] args) throws Exception {
//        ConnectionFactory factory = new ConnectionFactory();
//        factory.setHost("192.168.3.162");
//        factory.setPort(5672);
//        factory.setVirtualHost("/");
//        factory.setUsername("admin");
//        factory.setPassword("admin");
//        Connection connection = factory.newConnection();
//        Channel channel = connection.createChannel();
//        channel.queueDeclare("MAIN_QUEUE", true, false, false, null);
//        channel.queueBind("MAIN_QUEUE", "amq.direct", "MAIN_QUEUE");
//
//        channel.basicConsume("MAIN_QUEUE", true, "DELAY_QUEUE", new DefaultConsumer(channel) {
//            @Override
//            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
//                long deliveryTag = envelope.getDeliveryTag();
//                //do some work async
//                System.out.println(body[0]);
//            }
//        });

//        HashMap<String, Object> arguments = new HashMap<>();
//        arguments.put("x-dead-letter-exchange", "amq.direct");
//        arguments.put("x-dead-letter-routing-key", "MAIN_QUEUE");
//        channel.queueDeclare("DELAY_QUEUE", true, false, false, arguments);
//
//        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
//        AMQP.BasicProperties properties = builder.expiration(String.valueOf(1000)).deliveryMode(2).build();
//        channel.basicPublish("", "DELAY_QUEUE", properties, "test".getBytes("utf-8"));
        RabbitMQUtil.RabbitMQConsume consume = new RabbitMQUtil.RabbitMQConsume("192.168.3.162", 5672, "/", "admin", "admin", "MAIN_QUEUE", "DELAY_QUEUE", (RabbitMQUtil.IMessageHandler<byte[], String>) data -> {
            if (data == null) {
                return null;
            }
            return new String(data);
        });
        consume.start();
    }
}
