package com.order.bolt;

import java.util.Map;

import org.apache.log4j.Logger;

import com.order.util.FName;
import com.order.util.StreamId;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PageviewSplit extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;
	static Logger log = Logger.getLogger(PageviewSplit.class);

	@Override
	public void prepare(Map conf, TopologyContext context) {
		super.prepare(conf, context);
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String line = input.getString(0);
		String[] words = line.split("\\|", -1);
		if (words.length >= 57) {
			String remoteIp = words[0]; // remoteIp Varchar2(40)
			String recordTime = words[2]; // Recordtime Varchar2(20)
			String sessionId = words[39];// sessionId Varchar2(255)
			String userAgent = words[24]; // userAgent Varchar2(255)
			String pageType = words[2];// pageType Varchar2(8)
			String msisdn = words[9];// msisdn Varchar2(20)
			String channelCode = words[14]; // channelCode Varchar2(8)
			collector.emit(StreamId.ORDERDATA.name(), new Values(remoteIp,
					recordTime, sessionId, userAgent, pageType, msisdn,
					channelCode));
		} else {
			log.info("Error data: " + line);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(StreamId.ORDERDATA.name(),
				new Fields(FName.REMOTEIP.name(), FName.RECORDTIME.name(),
						FName.SESSIONID.name(), FName.USERAGENT.name(),
						FName.PAGETYPE.name(), FName.MSISDN.name(),
						FName.CHANNELCODE.name()));
	}
}
