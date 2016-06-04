package com.kafka.consumer;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

/**
 * Created by root on 4/6/16.
 */

final class KafkaConsumerThread implements Runnable {

    private KafkaStream stream;
    private int threadNo;

    public KafkaConsumerThread(KafkaStream stream, int threadNo) {
        this.threadNo = threadNo;
        this.stream = stream;
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            System.out.println("Consumed message from thread " + threadNo + ": " + new String(it.next().message()));
        }
        Thread t = Thread.currentThread();
        String name = t.getName();
        System.out.println("Thread Name: "+name+", stopping thread: " + threadNo);
    }

}
