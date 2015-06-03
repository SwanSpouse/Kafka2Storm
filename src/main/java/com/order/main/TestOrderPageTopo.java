package com.order.main;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import com.order.bolt.OrderDataTestBolt;
import com.order.util.StormConf;
import org.apache.log4j.Logger;
import storm.kafka.*;

/**
 * Created by LiMingji on 2015/6/3.
 */
public class TestOrderPageTopo {
    static Logger log = Logger.getLogger(Kafka2StormTopo.class);

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
        SpoutConfig spoutConfigTopic1 = new SpoutConfig(brokerHosts, topics[1], zkRoot, kafkaZkId);
        spoutConfigTopic1.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfigTopic1.forceFromStart = true;

        Config conf = new Config();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("KafkaSpout", new KafkaSpout(spoutConfigTopic1), 1);
        builder.setBolt("DataBolt", new OrderDataTestBolt(), 2).shuffleGrouping("KafkaSpout");

        // Run Topo on Cluster
        conf.setNumWorkers(2);
        StormSubmitter.submitTopology("TestOrderData", conf,builder.createTopology());
    }
}
