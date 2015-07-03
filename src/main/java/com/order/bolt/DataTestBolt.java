package com.order.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * Created by LiMingji on 2015/6/11.
 */
public class DataTestBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 0l;
    static Logger log = Logger.getLogger(SequenceBolt.class);

    String name;
    long id = 0l;
    public static final String STRING_SCHEME_KEY = "str";

    public DataTestBolt(String name) {
        this.name = name;
    }

    public DataTestBolt() {
        this.name = "first";
    }

    @Override
    public void prepare(Map conf, TopologyContext context) {
        super.prepare(conf, context);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = input.getStringByField(STRING_SCHEME_KEY);
        if (name.equals("second")) {
            log.info(name + "==>" + word);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
