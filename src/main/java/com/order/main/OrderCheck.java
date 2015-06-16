package com.order.main;


import backtype.storm.spout.SchemeAsMultiScheme;
import com.order.bolt.DataTestBolt;
import com.order.util.StormConf;
import com.order.util.StreamId;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;

public class OrderCheck {
	static Logger log = Logger.getLogger(OrderCheck.class);

    public static void main(String[] args) throws Exception {
        log.info("Start topology.");

        String zkCfg = StormConf.ZKCFG;
        String[] topics = StormConf.TOPIC;
        String zkRoot = StormConf.ZKROOT;
        String kafkaZkId = StormConf.ID;
        if (topics.length < 2) {
            log.error("Kafka's topic is less than 2.");
            System.exit(1);
        }

        BrokerHosts brokerHosts = new ZkHosts(zkCfg);
        SpoutConfig spoutConfigTopic1 = new SpoutConfig(brokerHosts, topics[0],
                zkRoot, kafkaZkId);
        spoutConfigTopic1.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfigTopic1.forceFromStart = false;

        SpoutConfig spoutConfigTopic2 = new SpoutConfig(brokerHosts, topics[1],
                zkRoot, kafkaZkId);
        spoutConfigTopic2.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfigTopic2.forceFromStart = false;

        Config conf = new Config();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(StreamId.TOPIC1.name(), new KafkaSpout(spoutConfigTopic1), 1);
        builder.setBolt(StreamId.BOLT1.name(), new DataTestBolt("first"), 2).shuffleGrouping(StreamId.TOPIC1.name());

        builder.setSpout(StreamId.TOPIC2.name(), new KafkaSpout(spoutConfigTopic2), 1);
        builder.setBolt(StreamId.BOLT2.name(), new DataTestBolt("second"), 2).shuffleGrouping(StreamId.TOPIC2.name());

        // Run Topo on Cluster
        conf.setNumWorkers(2);
        StormSubmitter.submitTopology("OrderCheck", conf, builder.createTopology());
    }
}
