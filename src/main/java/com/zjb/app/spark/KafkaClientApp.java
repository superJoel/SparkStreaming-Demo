package com.zjb.app.spark;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Joel
 */
public class KafkaClientApp {
    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(new KafkaProducer(KafkaProperties.TOPIC));
        executorService.execute(new KafkaConsumer(KafkaProperties.TOPIC));
    }
}
