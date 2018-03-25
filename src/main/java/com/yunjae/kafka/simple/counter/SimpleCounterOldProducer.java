package com.yunjae.kafka.simple.counter;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.producer.KeyedMessage;


import java.util.Properties;

import java.util.Scanner;

/**
 * brokerList : localhost:9092
 * topic : first
 * sync : sync, async
 * delay : 500
 * count : 10
 */
public class SimpleCounterOldProducer {
    private Properties kafkaProps = new Properties();
    private Producer<String, String> producer;
    private ProducerConfig config;
    private String topic;

    public static void main(String[] args) throws InterruptedException {
        SimpleCounterOldProducer counter = new SimpleCounterOldProducer();
        Scanner scanner = new Scanner(System.in);

        System.out.println("brokerList : ");
        String brokerList = scanner.nextLine();

        System.out.println("topic : ");
        String topic = scanner.nextLine();

        System.out.println("sync : ");
        String sync = scanner.nextLine();

        System.out.println("delay : ");
        long delay = Long.parseLong(scanner.nextLine());

        System.out.println("count : ");
        int count = Integer.parseInt(scanner.nextLine());

        counter.topic = topic;
        counter.configure(brokerList, sync);
        counter.start();

        long startTime = System.currentTimeMillis();
        System.out.println("Starting...");
        counter.produce("Starting....");

        for (int i=0; i<count; i++) {
            counter.produce(Integer.toString(i));
            Thread.sleep(delay);
        }

        long endTime = System.currentTimeMillis();
        System.out.println(".....and we are done. This took " + (endTime - startTime) + " ms");
        counter.produce(".....and we are done. This took " + (endTime - startTime) + " ms");

        counter.producer.close();
        System.exit(0);
    }


    private void configure(String brokerList, String sync) {
        kafkaProps.put("metadata.broker.list", brokerList);
        kafkaProps.put("serializer.class", "kafka.serializer.StringEncoder");
        kafkaProps.put("request.required.acks", "1");
        kafkaProps.put("producer.type", sync);
        kafkaProps.put("send.buffer.bytes","550000");
        kafkaProps.put("receive.buffer.bytes","550000");

        config = new ProducerConfig(kafkaProps);
    }

    private void start() {
        producer = new Producer<String, String>(config);
    }


    private void produce(String s) {
        KeyedMessage<String, String> message = new KeyedMessage<String, String>(topic, null, s);
        producer.send(message);
    }

    private void close() {
        producer.close();
    }

}

