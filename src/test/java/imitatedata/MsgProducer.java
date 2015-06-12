package imitatedata;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by LiMingji on 2015/6/12.
 */
public class MsgProducer {
    private final Producer<Integer, String> producer;
    private final Properties props = new Properties();

    public MsgProducer() {
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "10.1.69.179:9092");
        producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
    }

    public void sendMsg(String topic, String msg) {
        producer.send(new KeyedMessage<Integer, String>(topic, msg));
    }
}
