package com.xqt.kafkalab;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Main {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 3.批量发送
        // 达到消息延迟时间, 发送
        props.put("linger.ms", 1000);
        // 达到batch size, 发送
        props.put("Batch.size", 5);

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 20; i++) {
            // 1.异步
            producer.send(new ProducerRecord<>("test", Integer.toString(i), Integer.toString(i)));
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // 2.同步
            // Future<RecordMetadata> result = producer.send(new ProducerRecord<>("test", "" + (i % 5), Integer.toString(i)));
            // try {
            //     // 对消息进行阻塞
            //     RecordMetadata recordMetadata = result.get();
            // } catch (InterruptedException e) {
            //     e.printStackTrace();
            // } catch (ExecutionException e) {
            //     e.printStackTrace();
            // }
        }
        producer.close();
    }
}