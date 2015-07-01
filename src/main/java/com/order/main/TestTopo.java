package com.order.main;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.order.bolt.*;
import com.order.util.FName;
import com.order.util.StormConf;
import com.order.util.StreamId;
import org.apache.log4j.Logger;

import testspout.DataWarehouseSpout;

/**
 * Created by ghb on 2015/6/5.
 */
public class TestTopo {

    static Logger log = Logger.getLogger(TestTopo.class);

    public static void main(String[] args) throws Exception {
        log.info("Start topology.");

/*      
        String zkCfg = StormConf.ZKCFG;
        String[] topics = StormConf.TOPIC;
        String zkRoot = StormConf.ZKROOT;
        String kafkaZkId = StormConf.ID;
        if (topics.length < 2) {
            log.error("Kafka's topic is less than 2.");
            System.exit(1);
        }
        BrokerHosts brokerHosts = new ZkHosts(zkCfg);

        //浏览话单
        SpoutConfig pageViewSpoutConfigTopic = new SpoutConfig(brokerHosts, topics[0], zkRoot, kafkaZkId);
        pageViewSpoutConfigTopic.scheme = new SchemeAsMultiScheme(new StringScheme());
        pageViewSpoutConfigTopic.forceFromStart = false;

        //订购话单
        SpoutConfig orderSpoutConfigTopic = new SpoutConfig(brokerHosts, topics[1], zkRoot, kafkaZkId);
        orderSpoutConfigTopic.scheme = new SchemeAsMultiScheme(new StringScheme());
        orderSpoutConfigTopic.forceFromStart = false;
*/
        
        Config conf = new Config();
        TopologyBuilder builder = new TopologyBuilder();


        //浏览话单发射、分词bolt
        //builder.setSpout(StreamId.Portal_Pageview.name(), new KafkaSpout(pageViewSpoutConfigTopic), 3);
        //builder.setBolt(StreamId.PageViewSplit.name(), new PageviewSplit(), 20)
        //        .shuffleGrouping(StreamId.Portal_Pageview.name());

        //订购话单发射、分词bolt
        //builder.setSpout(StreamId.report_cdr.name(), new KafkaSpout(orderSpoutConfigTopic), 1);
        //builder.setBolt(StreamId.OrderSplit.name(), new OrderSplit(), 20)
        //        .shuffleGrouping(StreamId.report_cdr.name());

        
        //统计bolt
        //builder.setBolt(StreamId.StatisticsBolt.name(), new StatisticsBolt(), 20)
        //        .fieldsGrouping(StreamId.PageViewSplit.name(), StreamId.BROWSEDATA.name(), new Fields(FName.MSISDN.name()))
        //        .fieldsGrouping(StreamId.OrderSplit.name(), StreamId.ORDERDATA.name(), new Fields(FName.MSISDN.name()));

        builder.setSpout("testspout", new DataWarehouseSpout(), 1);

        builder.setBolt(StreamId.StatisticsBolt.name(), new StatisticsBolt(), 1);

        //仓库入库bolt
        builder.setBolt(StreamId.DataWarehouseBolt.name(), new DataWarehouseBolt(), 1)
                .fieldsGrouping("testspout", StreamId.DATASTREAM.name(), new Fields(FName.MSISDN.name()))
                .fieldsGrouping("testspout", StreamId.ABNORMALDATASTREAM.name(), new Fields(FName.MSISDN.name()));
        //实时输出接口bolt
        builder.setBolt(StreamId.RealTimeOutputBolt.name(), new RealTimeOutputBolt(), 1)
                .shuffleGrouping(StreamId.DataWarehouseBolt.name(), StreamId.DATASTREAM2.name())
                .shuffleGrouping(StreamId.DataWarehouseBolt.name(), StreamId.ABNORMALDATASTREAM2.name());

        // Run Topo on Cluster
        conf.setNumWorkers(1);
        conf.setNumAckers(0);
        StormSubmitter.submitTopology(StormConf.TOPONAME, conf, builder.createTopology());
    }
}
