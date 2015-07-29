package com.order.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.order.util.StormConf;
import com.order.util.StreamId;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * 用来替换simple level 的api。
 *
 * Created by LiMingji on 15/7/29.
 */
public class KafkaSpout extends BaseRichSpout {
    private transient ConsumerConnector consumer;
    private SpoutOutputCollector collector;
    private static Logger log = Logger.getLogger(KafkaSpout.class);

    //消息缓存队列。用来控制消息收发速度。
    private LinkedBlockingDeque<String> queue = null;
    private transient Thread msgEmitter = null;

    private String topic = null;
    private String streamId = null;

    public KafkaSpout(String topic) {
        this.topic = topic;
        //根据topi名称来判断streamId 的名称。
        streamId = this.topic.equals("Portal.Pageview") ? StreamId.Portal_Pageview.name() : StreamId.report_cdr.name();

        consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
        queue = new LinkedBlockingDeque<String>();
        msgEmitter = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    String message = queue.pop();
                    if (message != null) {
                        collector.emit(streamId, new Values(message));
                    }
                }
            }
        });
        msgEmitter.setDaemon(true);
        msgEmitter.start();
    }

    private ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", StormConf.ZKCFG);
        props.put("group.id", StormConf.GROUPID);
        props.put("zookeeper.session.timeout.ms", "12000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        consumer = consumer == null ? Consumer.createJavaConsumerConnector(createConsumerConfig()) : consumer;

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        while (it.hasNext()) {
            queue.push(new String(it.next().message()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(streamId, new Fields(streamId));
    }
}
