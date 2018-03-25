package com.yunjae.kafka.simple.counter;

import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.commons.collections.buffer.CircularFifoBuffer;

import java.util.*;

/**
 * zookeeper : localhost:2181
 * group.id : avg
 * topic : first
 * window-size: 10
 * wait-time : 120000
 */
public class SimpleMovingAvgZkConsumer {
    private Properties kafkaProps = new Properties();
    private ConsumerConnector consumer;
    private ConsumerConfig config;
    private KafkaStream<String, String> stream;
    private String waitTime;

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Input args : {zookeeper} {group.id} {topic} {window-size} {wait-time}");
        String scanin = scanner.nextLine();
        args = scanin.split(" ");

        if(args.length == 5) {
            System.out.println("SimpleMovingAvgZkConsumer args : {zookeeper} {group.id} {topic} {window-size} {wait-time}");
            return;
        }

        String next;
        int num;
        SimpleMovingAvgZkConsumer movingAvg = new SimpleMovingAvgZkConsumer();
        String zkUrl = args[0];
        String groupId = args[1];
        String topic = args[2];
        int window = Integer.parseInt(args[3]);
        movingAvg.waitTime = args[4];

        CircularFifoBuffer buffer = new CircularFifoBuffer(window);
        movingAvg.configure(zkUrl, groupId);
        movingAvg.start(topic);

        while ((next = movingAvg.getNextMessage()) != null) {
            int sum = 0;

            try {
                num = Integer.parseInt(next);
                buffer.add(num);
            } catch (NumberFormatException e) {
            }

            for(Object o : buffer) {
                sum += (Integer) o;
            }

            if (buffer.size() > 0) {
                System.out.println("Moving avg is : " + (sum/buffer.size()));
            }
        }

        movingAvg.consumer.shutdown();
        System.exit(0);
    }

    private void configure(String zkUrl, String groupId) {
        kafkaProps.put("zookeeper.connect", zkUrl);
        kafkaProps.put("group.id",groupId);
        kafkaProps.put("auto.commit.interval.ms","1000");
        kafkaProps.put("auto.offset.reset","largest");

        // un-comment this if you want to commit offsets manually
        //kafkaProps.put("auto.commit.enable","false");

        // un-comment this if you don't want to wait for data indefinitely
        kafkaProps.put("consumer.timeout.ms",waitTime);

        config = new ConsumerConfig(kafkaProps);
    }

    private void start(String topic) {
        consumer = Consumer.createJavaConsumerConnector(config);
        /* We tell Kafka how many threads will read each topic. We have one topic and one thread */
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic,new Integer(1));

        /* We will use a decoder to get Kafka to convert messages to Strings
         * valid property will be deserializer.encoding with the charset to use.
         * default is UTF8 which works for us */
        StringDecoder decoder = new StringDecoder(new VerifiableProperties());

        /* Kafka will give us a list of streams of messages for each topic.
        In this case, its just one topic with a list of a single stream */
        stream = consumer.createMessageStreams(topicCountMap, decoder, decoder).get(topic).get(0);
    }

    private String getNextMessage() {
        ConsumerIterator<String, String> it = stream.iterator();

        try {
            return it.next().message();
        } catch (ConsumerTimeoutException e) {
            System.out.println("waited " + waitTime + " and no messages arrived.");
            return null;
        }
    }



}
