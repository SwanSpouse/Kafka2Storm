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

public class OrderSplit extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;
	static Logger log = Logger.getLogger(OrderSplit.class);

	@Override
	public void prepare(Map conf, TopologyContext context) {
		super.prepare(conf, context);
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String line = input.getString(0);
		String[] words = line.split("\\|", -1);
		if (words.length >= 49) {
			String msisdn = words[0]; // msisdnID Varchar2(20)
			String recordTime = words[1]; // Recordtime Varchar2(14)
			String sessionId = words[39];// sessionId Varchar2(255)
			String wapIp = words[24]; // IP地址 Varchar2(40)
			String terminal = words[2];// UA Varchar2(255)
			String channelCode = words[9];// 渠道ID Varchar2(8)
			String cost = words[14]; // 费用 Number(12,4)
			String orderType = words[4]; // 订购类型  number(2)
			String productID = words[5];// 产品ID Varchar2(32)
			String bookID = words[7]; // 图书ID Number(19)
			String chapterID = words[8]; // 章节ID Varchar2(32)
			String promotionid = words[40]; // 营销参数 Number(19)
			collector.emit(StreamId.ORDERDATA.name(), new Values(msisdn,
					recordTime, sessionId, wapIp, terminal, channelCode, cost,
					orderType, productID, bookID, chapterID, promotionid));
		} else {
			log.info("Error data: " + line);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(StreamId.ORDERDATA.name(),
				new Fields(FName.MSISDN.name(), FName.RECORDTIME.name(),
						FName.SESSIONID.name(), FName.WAPIP.name(),
						FName.TERMINAL.name(), FName.CHANNELCODE.name(),
						FName.COST.name(), FName.ORDERTYPE.name(),
						FName.PRODUCTID.name(), FName.BOOKID.name(),
						FName.CHAPTERID.name(), FName.PROMOTIONID.name()));
	}
}
