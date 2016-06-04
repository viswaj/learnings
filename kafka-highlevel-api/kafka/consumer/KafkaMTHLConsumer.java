package com.kafka.consumer;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by root on 4/6/16.
 */
public class KafkaMTHLConsumer {

    private ExecutorService executorPool;
    private final ConsumerConnector consumer;
    private final String topic;
    private final String POOL_NAME="kafkapool";

    public KafkaMTHLConsumer(String zookeeper, String groupId, String topic) {
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

    public void consumeData(int threadCount) {
        //Map<String, Integer> topicCount = new HashMap<>();
        //topicCount.put(topic, threadCount);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(ImmutableMap.of(topic,threadCount));
        List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);

        //we can use cachedthreadpool and threadCount not req.
        executorPool = Executors.newFixedThreadPool(threadCount, new ThreadFactoryBuilder().setNameFormat(POOL_NAME).build());
        //executorPool = Executors.newCachedThreadPool();

        int threadNumber = 0;
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            executorPool.submit(new KafkaConsumerThread(stream, threadNumber));
            threadNumber++;
        }

        try {
            Thread.sleep(20000);
        } catch (InterruptedException ie) {

        }
        if (consumer != null) {
            consumer.shutdown();
        }
        if (executorPool != null) {
            executorPool.shutdown();
        }
    }

    public static void main(String[] args) {
        String topic = "kafka-test";
        int threadCount = 3;
        KafkaMTHLConsumer kafkaMTHLConsumer = new KafkaMTHLConsumer("localhost:2181", "myconsumergroup", topic);
        kafkaMTHLConsumer.consumeData(threadCount);
    }

}
