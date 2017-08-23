package com.oneandone.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * created by mincekara
 */
public class ConsumerLoop implements Runnable{
    private final KafkaConsumer<String, String> consumer;
    private final List<String> topics;
    private final int id;

    public ConsumerLoop(int id, String groupId, List<String> topics) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.88.10.75:9092");
        props.put("group.id",groupId);
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        this.id = id;
        this.topics = topics;
        this.consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run(){
        consumer.subscribe(topics);

        try {
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records){
                    Map<String, Object> data = new HashMap<>();
                    data.put("partition", record.partition());
                    data.put("offset", record.offset());
                    data.put("value", record.value());

                    System.out.println(this.id + ": " + data);
                }
            }
        } catch (WakeupException e) {
            //ignore for shutdown
        } finally {
            consumer.close();
        }
    }

    public void shutdown(){
        consumer.wakeup();
    }

    public static void main(String[] args) {
        int numConsumers = 1;
        String groupId = "test";
        List<String> topics = Arrays.asList("test");
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

        final List<ConsumerLoop> consumers = new ArrayList<>();
        for (int i = 0; i < numConsumers; i++) {
            ConsumerLoop consumer = new ConsumerLoop(i, groupId, topics);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run(){
                for (ConsumerLoop consumer : consumers){
                    consumer.shutdown();
                }
                executor.shutdown();

                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });
    }
}
