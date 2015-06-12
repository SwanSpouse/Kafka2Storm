package imitatedata;

import java2kafka.*;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by LiMingji on 2015/6/12.
 */
public class ConsumerMain {
    private static ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", java2kafka.KafkaProperties.zkConnect);
        props.put("group.id", java2kafka.KafkaProperties.groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);

    }

    public static void main(String[] args) throws InterruptedException {
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(KafkaProperties.viewTopic, new Integer(1));

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(KafkaProperties.viewTopic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (true) {
            Thread.sleep(1 * 100L);
            while (it.hasNext())
                System.out.println(new String(it.next().message()));
        }
    }
}





















