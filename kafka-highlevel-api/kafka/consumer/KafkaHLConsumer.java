package com.kafka.consumer;

/**
 * Created by root on 4/6/16.
 */

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaHLConsumer {

    private final ConsumerConnector consumer;
    private final String topic;

    public KafkaHLConsumer(String zookeeper, String groupId, String topic) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "500");
        props.put("zookeeper.sync.time.ms", "250");
        props.put("auto.commit.interval.ms", "1000");
        props.put("autooffset.reset","smallest");

        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        this.topic = topic;
    }

    public void consumeData() {
        System.out.println("calling me");
        Map<String, Integer> topicCount = new HashMap<>();
        topicCount.put(topic, 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
        List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);
        for (final KafkaStream<byte[], byte[]> stream : streams) {/*
            for (MessageAndMetadata<byte[], byte[]> message : stream) {
                System.out.println("Message from Single Topic: " + new String(message.message()));
            }*/
            ConsumerIterator<byte[], byte[]> it=stream.iterator();

            while (it.hasNext()) {
                System.out.println("Consumed message: " + new String(it.next().message()));
            }
        }
        if (consumer != null) {
            consumer.shutdown();
        }
    }

    public static void main(String[] args) {
        String topic = "test";
        KafkaHLConsumer kafkaHLConsumer = new KafkaHLConsumer("127.0.0.1:2181", "myconsumergroup", topic);
        kafkaHLConsumer.consumeData();
    }

}
