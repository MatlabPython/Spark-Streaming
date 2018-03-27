package com.gsafety.lifeline.bigdata.streaming.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created with Intellij IDEA.
 * User: naichun
 * Date: 2018-01-18
 * Time: 12:03
 */
public class KafkaCopy {
    public static void main(String[] args){
        if (args.length != 4) {
            System.out.println("args: groupId sendTopicName threads offset");
        }
        String groupId = args[0];
        String sendTopic = args[1];
        String topics = "CLLSEN-BRIDGE-HF-ACCE-DR,CLLSEN-BRIDGE-HF-STRAIN-DR";
        int threads = Integer.parseInt(args[2]);
        String offset=args[3];
        Properties proConf = new Properties();
        proConf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.5.4.40:9092,10.5.4.41:9092");
        proConf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        proConf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(proConf);
        for(int i=0;i<threads;i++) {
            new Thread(new ConsumerRunnable(producer, topics, groupId, sendTopic,offset)).start();
        }
    }

}

class ConsumerRunnable implements Runnable {

    private long count = 0L;
    private KafkaProducer<String, byte[]> producer;
    private KafkaConsumer<String, byte[]> consumer;
    private String sendTopic;
    private boolean wait;
    public ConsumerRunnable(KafkaProducer<String, byte[]> producer, String topics, String groupId,String sendTopic,String offset) {
        this.producer = producer;
        this.sendTopic = sendTopic;
        this.wait = "earliest".equals(offset);
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.5.4.40:9092,10.5.4.41:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,offset);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        this.consumer = new KafkaConsumer<String, byte[]>(properties);
        this.consumer.subscribe(Arrays.asList((topics).split(",")));
    }
    public void run() {
        long start = System.currentTimeMillis();
        long count = 0L;
        while(true) {
            ConsumerRecords<String, byte[]> records = this.consumer.poll(100L);
            for (ConsumerRecord<String, byte[]> record : records) {
                producer.send(new ProducerRecord<String, byte[]>(this.sendTopic,null, record.value()));
                count++;
            }
            if (wait) {
                if(count>200000){
                    long end = System.currentTimeMillis();
                    System.out.println("copy speed "+(count*1000/(end-start)/10000)+"w/s");
                    count = 0L;
                    start = end;
                    try {
                        Thread.sleep(1500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }else{
                if(count>10000){
                    long end = System.currentTimeMillis();
                    System.out.println("copy speed "+(count*1000/(end-start))+"/s");
                    count = 0L;
                    start = end;
                }
            }
        }
    }
}
