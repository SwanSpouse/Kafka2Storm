package com.order.spout;

import java.util.Map;

import org.apache.log4j.Logger;

import com.order.util.FName;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class InputSpout extends BaseRichSpout{

	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	static Logger log = Logger.getLogger(InputSpout.class);

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	public void nextTuple() {
	}


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FName.LINE.name()));
	}

	public void close() {
	}

	public void ack(Object msgId) {
		super.ack(msgId);
	}
}
