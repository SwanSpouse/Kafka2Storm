package com.order.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

import java.util.Map;

/**
 * 页面阅读Topic
 * Created by LiMingji on 2015/5/24.
 */
public class KafkaSpout extends BaseRichSpout {
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

    }

    @Override
    public void nextTuple() {

    }
}
