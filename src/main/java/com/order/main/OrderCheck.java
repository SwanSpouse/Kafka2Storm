package com.order.main;


import org.apache.log4j.Logger;

import com.order.bolt.AnalysisBolt;
import com.order.spout.InputSpout;
import com.order.util.ID;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class OrderCheck {
	static Logger log = Logger.getLogger(OrderCheck.class);
	public static final int TimeSeconds = 30;

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(ID.InputSpout.name(), new InputSpout(), 1);
		builder.setBolt(ID.AnalysisBolt.name(), new AnalysisBolt(), 2)
				.shuffleGrouping(ID.InputSpout.name());

		Config conf = new Config();
		conf.setNumWorkers(2);
		StormSubmitter.submitTopology("OrderTest", conf,
				builder.createTopology());
	}
}
