package com.zjb.app.spark;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * kafka消费者
 *
 * @author Joel
 */
public class KafkaConsumer implements Runnable {
    private String topic;

    public KafkaConsumer(String topic) {
        this.topic = topic;
    }

    private ConsumerConnector createConsumerConnector() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", KafkaProperties.ZK);
        properties.put("group.id", "test");
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    public void run() {
        ConsumerConnector consumerConnector = createConsumerConnector();
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumerConnector.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
        while (iterator.hasNext()) {
            String msg = new String(iterator.next().message());
            System.out.println("rec: " + msg);
        }
    }
}
