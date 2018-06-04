package com.zjb.app.spark;

import org.apache.log4j.Logger;

public class GenerateLogApp {
    private static Logger logger = Logger.getLogger(GenerateLogApp.class.getName());

    public static void main(String[] args) throws InterruptedException {
        int index = 0;
        while (true) {
            Thread.sleep(1000L);
            logger.info("value is " + index);
            index++;
        }
    }
}
