package com.order.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;

/**
 * Created by LiMingji on 2015/6/3.
 */
public class OrderDataTestBolt extends BaseBasicBolt {

    static Logger log = Logger.getLogger(OrderDataTestBolt.class);
    public static final String STRING_SCHEME_KEY = "str";

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String value = input.getStringByField(STRING_SCHEME_KEY);
        log.info("=========== 数据 ===========");
        log.info("value " + value);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //do nothing
    }
}
