package net.sea.mq.dalay;

import com.rabbitmq.client.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

/**
 * @author huacw
 * @date 2019/6/27
 */
public class RabbitMQUtil {
    /**
     * 构建连接工厂
     *
     * @param host
     * @param port
     * @param vHost
     * @param username
     * @param password
     * @return
     */
    private static ConnectionFactory buildConnectionFactory(String host, int port, String vHost, String username, String password) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setVirtualHost(vHost);
        factory.setUsername(username);
        factory.setPassword(password);
        return factory;
    }

    /**
     * 打开连接
     *
     * @param factory
     * @return
     * @throws IOException
     * @throws TimeoutException
     */
    private static Connection openConnection(ConnectionFactory factory) throws IOException, TimeoutException {
        if (factory == null) {
            throw new RuntimeException("获取连接工厂失败");
        }
        return factory.newConnection();
    }

    /**
     * 创建channel
     *
     * @param connection
     * @return
     * @throws IOException
     */
    private static Channel newChannel(Connection connection) throws IOException {
        if (connection == null) {
            throw new RuntimeException("获取连接失败");
        }
        return connection.createChannel();
    }

    /**
     * 消息解析器
     *
     * @param <P>
     * @param <R>
     */
    public static interface IMessageHandler<P, R> {
        /**
         * 解析消息数据
         *
         * @param data
         * @return
         */
        R dealData(P data);
    }

    /**
     * rabbitMQ消费者对象
     */
    public static class RabbitMQConsume {
        private Channel channel;
        private String mainChannel;
        private String subChannel;
        private IMessageHandler handler;

        public RabbitMQConsume(String host, int port, String vHost, String username, String password, String mainChannel, String subChannel, IMessageHandler handler) throws IOException, TimeoutException {
            channel = newChannel(openConnection(buildConnectionFactory(host, port, vHost, username, password)));
            this.mainChannel = mainChannel;
            this.subChannel = subChannel;
            this.handler = handler;
        }

        /**
         * 启动监听器
         *
         * @throws IOException
         */
        public void start() throws IOException {
            channel.queueDeclare(mainChannel, true, false, false, null);
            channel.queueBind(mainChannel, "amq.direct", mainChannel);

            channel.basicConsume(mainChannel, true, subChannel, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    //TODO 处理延时消息
                    //do some work async
                    System.out.println(handler.dealData(body));
                }
            });
        }
    }

    /**
     * rabbitmq生产者
     */
    public static class RabbitMQProducer<P> implements Closeable {
        private Channel channel;
        private String mainChannel;
        private String subChannel;
        private IMessageHandler<P, byte[]> handler;
        private Connection connection;

        public RabbitMQProducer(String host, int port, String vHost, String username, String password, String mainChannel, String subChannel, IMessageHandler<P, byte[]> handler) throws IOException, TimeoutException {
            connection = openConnection(buildConnectionFactory(host, port, vHost, username, password));
            channel = newChannel(connection);
            this.mainChannel = mainChannel;
            this.subChannel = subChannel;
            this.handler = handler;
        }

        public void send(P message, int timeout) throws IOException, TimeoutException {
            HashMap<String, Object> arguments = new HashMap<>();
            arguments.put("x-dead-letter-exchange", "amq.direct");
            arguments.put("x-dead-letter-routing-key", mainChannel);
            channel.queueDeclare(subChannel, true, false, false, arguments);

            AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();

            byte[] messageBodyBytes = "Hello, world!".getBytes();
            timeout = timeout <= 0 ? 1000 : timeout;
            AMQP.BasicProperties basicProperties = new AMQP.BasicProperties.Builder().expiration(String.valueOf(timeout)).build();
            channel.basicPublish("", subChannel, basicProperties, handler.dealData(message));
        }

        @Override
        public void close() throws IOException {
            if (channel != null) {
                try {
                    channel.close();
                } catch (TimeoutException e) {
                    e.printStackTrace();
                    channel = null;
                }
            }
            if (connection != null) {
                connection.close();
            }
        }
    }
}
