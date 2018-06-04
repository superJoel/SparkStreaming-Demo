package com.zjb.app.spark;

import jdk.nashorn.internal.ir.annotations.Ignore;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * kafka生产者
 *
 * @author Joel
 */
public class KafkaProducer implements Runnable {
    private String topic;

    private Producer<Integer, String> producer;

    public KafkaProducer(String topic) {
        this.topic = topic;
        Properties properties = new Properties();
        properties.put("metadata.broker.list", KafkaProperties.BROKER_LIST);
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");
        producer = new Producer<Integer, String>(new ProducerConfig(properties));
    }

    public void run() {
        int messageNo = 1;
        try {
            while (true) {
                String msg = "Message_" + messageNo;
                producer.send(new KeyedMessage<Integer, String>(topic, msg));
                System.out.println("Sent:" + msg);
                messageNo++;
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } finally {
            producer.close();
        }
    }
}
