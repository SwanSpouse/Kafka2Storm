package com.order.main;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.order.bolt.*;
import com.order.spout.KafkaSpout;
import com.order.util.FName;
import com.order.util.StormConf;
import com.order.util.StreamId;
import org.apache.log4j.Logger;

/**
 * Created by LiMingji on 2015/7/29.
 */
/**
 ////////////////////////////////////////////////////////////////////
 //							_ooOoo_								  //
 //						   o8888888o							  //
 //						   88" . "88							  //
 //						   (| ^_^ |)							  //
 //						   O\  =  /O							  //
 //						____/`---'\____							  //
 //					  .'  \\|     |//  `.						  //
 //					 /  \\|||  :  |||//  \						  //
 //				    /  _||||| -:- |||||-  \						  //
 //				    |   | \\\  -  /// |   |						  //
 //					| \_|  ''\---/''  |   |						  //
 //					\  .-\__  `-`  ___/-. /						  //
 //				  ___`. .'  /--.--\  `. . ___					  //
 //				."" '<  `.___\_<|>_/___.'  >'"".				  //
 //			  | | :  `- \`.;`\ _ /`;.`/ - ` : | |				  //
 //			  \  \ `-.   \_ __\ /__ _/   .-` /  /                 //
 //		========`-.____`-.___\_____/___.-`____.-'========		  //
 //				             `=---='                              //
 //		^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^        //
 //         佛祖保佑           永无BUG		         永不修改			  //
 ////////////////////////////////////////////////////////////////////
 */

public class SimpleLevelTopo {

    static Logger log = Logger.getLogger(ExceptOrderTopo.class);

    public static void main(String[] args) throws Exception {
        log.info("Start topology.");
        String[] topics = StormConf.TOPIC;
        if (topics.length < 2) {
            log.error("Kafka's topic is less than 2.");
            System.exit(1);
        }

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
        builder.setSpout(StreamId.Portal_Pageview.name(), new KafkaSpout(topics[0]), 16);
        builder.setBolt(StreamId.PageViewSplit.name(), new PageviewSplit(), 50)
                .shuffleGrouping(StreamId.Portal_Pageview.name());

        //订购话单发射、分词bolt
        builder.setSpout(StreamId.report_cdr.name(), new KafkaSpout(topics[1]), 8);
        builder.setBolt(StreamId.OrderSplit.name(), new OrderSplit(), 5)
                .shuffleGrouping(StreamId.report_cdr.name());

        //缓存bolt
        builder.setBolt(StreamId.MessageBufferBolt.name(), new MessageBufferBolt(), 50)
                .fieldsGrouping(StreamId.PageViewSplit.name(), StreamId.BROWSEDATA.name(), new Fields(FName.MSISDN.name()))
                .fieldsGrouping(StreamId.OrderSplit.name(), StreamId.ORDERDATA.name(), new Fields(FName.MSISDN.name()));

        //统计bolt
        builder.setBolt(StreamId.StatisticsBolt.name(), new StatisticsBolt(), 100)
                .fieldsGrouping(StreamId.MessageBufferBolt.name(), StreamId.BROWSEDATA.name(), new Fields(FName.MSISDN.name()))
                .fieldsGrouping(StreamId.MessageBufferBolt.name(), StreamId.ORDERDATA.name(), new Fields(FName.MSISDN.name()));

        //仓库入库bolt
        builder.setBolt(StreamId.DataWarehouseBolt.name(), new DataWarehouseBolt(), 6)
                .fieldsGrouping(StreamId.StatisticsBolt.name(), StreamId.DATASTREAM.name(), new Fields(FName.MSISDN.name()))
                .fieldsGrouping(StreamId.StatisticsBolt.name(), StreamId.ABNORMALDATASTREAM.name(), new Fields(FName.MSISDN.name()));
        //实时输出接口bolt
        builder.setBolt(StreamId.RealTimeOutputBolt.name(), new RealTimeOutputBolt(), 6)
                .fieldsGrouping(StreamId.DataWarehouseBolt.name(), StreamId.DATASTREAM2.name(),
                        new Fields(FName.CHANNELCODE.name(), FName.PROVINCEID.name(), FName.CONTENTID.name(), FName.CONTENTTYPE.name()))
                .fieldsGrouping(StreamId.DataWarehouseBolt.name(), StreamId.ABNORMALDATASTREAM2.name(),
                        new Fields(FName.CHANNELCODE.name(), FName.PROVINCEID.name(), FName.CONTENTID.name(), FName.CONTENTTYPE.name()));

        // Run Topo on Cluster
        conf.setNumWorkers(20);
        conf.setNumAckers(0);
        conf.setMaxSpoutPending(100000);
        conf.setMessageTimeoutSecs(60000);
        conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE,             8);
        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE,            32);
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,    16384);
        StormSubmitter.submitTopology("SimpleLevelSpout", conf, builder.createTopology());
    }
}
