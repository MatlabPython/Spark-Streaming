package com.gsafety.lifeline.bigdata.kafka;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import com.gsafety.lifeline.bigdata.avro.SensorDetail;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by hadoop on 2017/9/18.
 */
public class KafkaProducer implements Serializable{
    public static final String METADATA_BROKER_LIST_KEY = "metadata.broker.list";
    public static final String SERIALIZER_CLASS = "serializer.class";
    public static final String SERIALIZER_CLASS_VALUE = "kafka.serializer.DefaultEncoder";
    public static final String KEY_SERIALIZER_CLASS = "key.serializer.class";
    public static final String KEY_SERIALIZER_CLASS_VALUE = "kafka.serializer.StringEncoder";
    //public static final String SERIALIZER_CLASS_VALUE = "com.gsafety.lifeline.bigdata.kafka.KeywordMessage";
    private static KafkaProducer instance = null;

    private Producer producer;
    private KafkaProducer(String brokerList) {

        Preconditions.checkArgument(StringUtils.isNotBlank(brokerList), "kafka brokerList is blank...");

        // set properties
        Properties properties = new Properties();
        properties.put(METADATA_BROKER_LIST_KEY, brokerList);
        properties.put(SERIALIZER_CLASS, SERIALIZER_CLASS_VALUE);
        properties.put(KEY_SERIALIZER_CLASS, KEY_SERIALIZER_CLASS_VALUE);
        properties.put("kafka.message.CompressionCodec", "1");
        properties.put("client.id", "streaming-kafka-output");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        this.producer = new Producer(producerConfig);
    }
    public static synchronized KafkaProducer getInstance(String brokerList) {
        if (instance == null) {
            instance = new KafkaProducer(brokerList);
            System.out.println("初始化 kafka producer...");
        }
        return instance;
    }

    // 单条发送
    public void send(KeyedMessage<String, byte[]> keyedMessage) {
        producer.send(keyedMessage);
    }

    // 批量发送
    public void send(List<KeyedMessage<String,byte[]>> keyedMessageList) {
        producer.send(keyedMessageList);
    }

    public void shutdown() {
        producer.close();
    }

}
