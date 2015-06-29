package com.order.main;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.order.bolt.*;
import com.order.main.Grouping.DataWarehouseGrouping;
import com.order.util.FName;
import com.order.util.StormConf;
import com.order.util.StreamId;
import org.apache.log4j.Logger;
import storm.kafka.*;

import java.awt.print.Book;

/**
 * Created by LiMingji on 2015/6/5.
 */
public class ExceptOrderTopo {

    static Logger log = Logger.getLogger(ExceptOrderTopo.class);

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

        //浏览话单
        SpoutConfig pageViewSpoutConfigTopic = new SpoutConfig(brokerHosts, topics[0], zkRoot, kafkaZkId);
        pageViewSpoutConfigTopic.scheme = new SchemeAsMultiScheme(new StringScheme());
        pageViewSpoutConfigTopic.forceFromStart = false;

        //订购话单
        SpoutConfig orderSpoutConfigTopic = new SpoutConfig(brokerHosts, topics[1], zkRoot, kafkaZkId);
        orderSpoutConfigTopic.scheme = new SchemeAsMultiScheme(new StringScheme());
        orderSpoutConfigTopic.forceFromStart = false;

        Config conf = new Config();
        TopologyBuilder builder = new TopologyBuilder();

        /**
         *   这里有意将浏览话单的并行度设置大于订购话单。是防止订购话单消费过快而浏览话单赶不上订购话单的速度。
         * e.g. 20150623130510的话单消费的时候有可能几秒前的浏览话单还没有消费。这样就会产生错误。因为话单消
         * 费的时候浏览记录还没有到达。
         *                                         2015-06-23 李洺吉  注
         *
         */
        //浏览话单发射、分词bolt
        builder.setSpout(StreamId.Portal_Pageview.name(), new KafkaSpout(pageViewSpoutConfigTopic), 3);
        builder.setBolt(StreamId.PageViewSplit.name(), new PageviewSplit(), 20)
                .shuffleGrouping(StreamId.Portal_Pageview.name());

        //订购话单发射、分词bolt
        builder.setSpout(StreamId.report_cdr.name(), new KafkaSpout(orderSpoutConfigTopic), 1);
        builder.setBolt(StreamId.OrderSplit.name(), new OrderSplit(), 20)
                .shuffleGrouping(StreamId.report_cdr.name());

        //统计bolt
        builder.setBolt(StreamId.StatisticsBolt.name(), new StatisticsBolt(), 20)
                .fieldsGrouping(StreamId.PageViewSplit.name(), StreamId.BROWSEDATA.name(), new Fields(FName.MSISDN.name()))
                .fieldsGrouping(StreamId.OrderSplit.name(), StreamId.ORDERDATA.name(), new Fields(FName.MSISDN.name()));

        //仓库入库bolt
        builder.setBolt(StreamId.DataWarehouseBolt.name(), new DataWarehouseBolt(), 20)
                .fieldsGrouping(StreamId.StatisticsBolt.name(), StreamId.DATASTREAM.name(), new Fields(FName.MSISDN.name()))
                .fieldsGrouping(StreamId.StatisticsBolt.name(), StreamId.ABNORMALDATASTREAM.name(), new Fields(FName.MSISDN.name()));
        //实时输出接口bolt
        builder.setBolt(StreamId.RealTimeOutputBolt.name(), new RealTimeOutputBolt(), 20)
                .shuffleGrouping(StreamId.DataWarehouseBolt.name(), StreamId.DATASTREAM2.name())
                .shuffleGrouping(StreamId.DataWarehouseBolt.name(), StreamId.ABNORMALDATASTREAM2.name());

        // Run Topo on Cluster
        conf.setNumWorkers(10);
        conf.setNumAckers(0);
        StormSubmitter.submitTopology(StormConf.TOPONAME, conf, builder.createTopology());
    }
}
