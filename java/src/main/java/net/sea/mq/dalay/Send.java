package net.sea.mq.dalay;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class Send {

    private final static String QUEUE_NAME = "DELAY_QUEUE";

    public static void main(String[] argv) throws Exception {
//        ConnectionFactory factory = new ConnectionFactory();
//        factory.setHost("192.168.3.162");
//        factory.setPort(5672);
//        factory.setVirtualHost("/");
//        factory.setUsername("admin");
//        factory.setPassword("admin");
//        Connection connection = factory.newConnection();
//        Channel channel = connection.createChannel();
//
//        HashMap<String, Object> arguments = new HashMap<>();
//        arguments.put("x-dead-letter-exchange", "amq.direct");
//        arguments.put("x-dead-letter-routing-key", "MAIN_QUEUE");
//        channel.queueDeclare("DELAY_QUEUE", true, false, false, arguments);
//
//        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
//        AMQP.BasicProperties properties = builder.expiration(String.valueOf(100)).deliveryMode(2).build();
//        //channel.basicPublish("", "DELAY_QUEUE", properties, "test".getBytes("utf-8"));
//
//        byte[] messageBodyBytes = "Hello, world!".getBytes();
//        byte i = 10;
//        while (i-- > 0) {
//            channel.basicPublish("", "DELAY_QUEUE", new AMQP.BasicProperties.Builder().expiration(String.valueOf(i * 1000)).build(),
//                    new byte[]{i});
//
//        }
//        channel.close();
//        connection.close();

        Random random = new Random();
        try (RabbitMQUtil.RabbitMQProducer<String> producer = new RabbitMQUtil.RabbitMQProducer("192.168.2.162", 5672, "/", "admin", "admin", "MAIN_QUEUE", "DELAY_QUEUE", (RabbitMQUtil.IMessageHandler<String, byte[]>) data -> {
            if (data == null || data.trim().length() == 0) {
                return new byte[0];
            }
            return data.getBytes();
        });) {
            byte i = 10;
            while (i-- > 0) {
                int timeout = random.nextInt(10) * 1000;
                System.out.println("timeout=" + timeout);
                producer.send("msg" + i, timeout);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
