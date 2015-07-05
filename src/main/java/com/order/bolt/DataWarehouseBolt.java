package com.order.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.order.db.DBHelper.DBDataWarehouseCacheHelper;
import com.order.util.FName;
import com.order.util.LogUtil;
import com.order.util.OrderRecord;
import com.order.util.StreamId;
import com.order.util.TimeParaser;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * 仓库接口。计算异常率，定时写入数据库
 *
 * 输出表结构:
 * CREATE TABLE "AAS"."RESULT_TABLE"
 * (
 * "record_time"   varchar2(8 byte),
 * "msisdn"        varchar2(32 byte),
 * "sessionid"     varchar2(40 byte),
 * "channelcode"   varchar2(40 byte),
 * "realfee"       NUMBER,
 * "rule_1"        varchar2(2 byte),
 * "rule_2"        varchar2(2 byte),
 * "rule_3"        varchar2(2 byte),
 * "rule_4"        varchar2(2 byte),
 * "rule_5"        varchar2(2 byte),
 * "rule_6"        varchar2(2 byte),
 * "rule_7"        varchar2(2 byte),
 * "rule_8"        varchar2(2 byte),
 * "rule_9"        varchar2(2 byte),
 * "rule_10"       varchar2(2 byte),
 * "rule_11"       varchar2(2 byte),
 * "rule_12"       varchar2(2 byte)
 * )
 *
 * Created by LiMingji on 2015/5/24.
 */
public class DataWarehouseBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;
	private DBDataWarehouseCacheHelper DBHelper = new DBDataWarehouseCacheHelper();

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		if (input.getSourceStreamId().equals(StreamId.DATASTREAM.name())) {
			handleDataStream(input, collector);
		} else if (input.getSourceStreamId().equals(
				StreamId.ABNORMALDATASTREAM.name())) {
			handleAbnormalDataStream(input, collector);
		}
	}

	// 处理正常数据流
	private void handleDataStream(Tuple input, BasicOutputCollector collector) {
		String msisdn = input.getStringByField(FName.MSISDN.name());
		String sessionId = input.getStringByField(FName.SESSIONID.name());
		String channelCode = input.getStringByField(FName.CHANNELCODE.name());
		Long recordTime = input.getLongByField(FName.RECORDTIME.name());
		String bookId = input.getStringByField(FName.BOOKID.name());
		String productId = input.getStringByField(FName.PRODUCTID.name());
		double realInfoFee = input.getDoubleByField(FName.REALINFORFEE.name());
        String provinceId = input.getStringByField(FName.PROVINCEID.name());
		int orderType = input.getIntegerByField(FName.ORDERTYPE.name());

        LogUtil.printLog("DataWareHouseBolt 接收正常数据流: " + msisdn + " " + recordTime + " " + realInfoFee);

		// 订购记录增加到内存
		if( DBHelper.insertData(msisdn, sessionId, channelCode, recordTime, bookId,
				productId, realInfoFee, provinceId, orderType) == 1) {
	        // 若新增成功则直接转发消息
	        collector.emit(StreamId.DATASTREAM2.name(), new Values(msisdn, sessionId, recordTime,
	                realInfoFee, channelCode, productId, provinceId, orderType, bookId));
		}
	}

	// 处理异常数据流
	private void handleAbnormalDataStream(Tuple input, BasicOutputCollector collector) {
		String msisdn = input.getStringByField(FName.MSISDN.name());
		String sessionId = input.getStringByField(FName.SESSIONID.name());
		String channelCode = input.getStringByField(FName.CHANNELCODE.name());
		Long recordTime = input.getLongByField(FName.RECORDTIME.name());
		String bookId = input.getStringByField(FName.BOOKID.name());
		String productId = input.getStringByField(FName.PRODUCTID.name());
		double realInfoFee = input.getDoubleByField(FName.REALINFORFEE.name());
		String provinceId = input.getStringByField(FName.PROVINCEID.name());
		int orderType = input.getIntegerByField(FName.ORDERTYPE.name());
		String rule = input.getStringByField(FName.RULES.name());

        LogUtil.printLog("DataWareHouseBolt 接收异常数据流: " + msisdn + " " + recordTime + " " + realInfoFee);

		// 如果update时找不到订购记录，则首先插入一条
        int result = DBHelper.updateData(msisdn, sessionId, channelCode, recordTime, bookId,
				productId, realInfoFee, provinceId, orderType, rule);
		if (result == -1) {
			// 订购记录增加到内存
			if( DBHelper.insertData(msisdn, sessionId, channelCode, recordTime, bookId,
					productId, realInfoFee, provinceId, orderType) == 1) {
		        // 若新增成功则直接转发正常订购消息
		        collector.emit(StreamId.DATASTREAM2.name(), new Values(msisdn, sessionId, recordTime,
		                realInfoFee, channelCode, productId, provinceId, orderType, bookId));
			}
			// 再次更新
			result = DBHelper.updateData(msisdn, sessionId, channelCode, recordTime, bookId,
					productId, realInfoFee, provinceId, orderType, rule);
		}
		// 更新成功后直接转发异常订购
		if (result == 1) {
			collector.emit(StreamId.ABNORMALDATASTREAM2.name(), new Values(msisdn, sessionId, recordTime, 
					realInfoFee, channelCode, productId, rule, provinceId, orderType, bookId));
		}

		// 向前追溯准备阶段
        int ruleId = DBDataWarehouseCacheHelper.getRuleNumFromString(rule);
        if (ruleId < 1 || ruleId > 12) {
            return;
        }
        /**
         * 1 2 3 5 6 7 8 这些规则是向前追溯该渠道下1小时数据。
           4    追溯一天的数据
           9 10 11 追溯自然小时的数据。
         */
        String traceBackTime = "";  // ?? spout中话单时间为空应直接干掉
        if (ruleId == 1 || ruleId == 2 || ruleId == 3 ||
                ruleId == 5 || ruleId == 6 || ruleId == 7 || ruleId == 8) {
            traceBackTime = TimeParaser.OneHourAgo(recordTime);
        } else if (ruleId == 4) {
            traceBackTime = TimeParaser.OneDayAgo(recordTime);
        } else if (ruleId == 9 || ruleId == 10 || ruleId == 11) {    //?? 无规则12
            traceBackTime = TimeParaser.NormalHourAgo(recordTime);
        } else {
			return;
		}
        
		// 向前回溯各个规则有不同的追溯参数  ??
		ArrayList<OrderRecord> list = DBHelper.traceBackOrders(msisdn,
				channelCode, TimeParaser.splitTime(traceBackTime), ruleId);
		// 将回溯的订购发送
		Iterator<OrderRecord> itOrder = list.iterator();
		while (itOrder.hasNext()) {
			OrderRecord oneRecord = itOrder.next();
			collector.emit(
					StreamId.ABNORMALDATASTREAM2.name(),
					new Values(oneRecord.getMsisdn(), oneRecord.getSessionId(),
							oneRecord.getRecordTime(), oneRecord.getRealfee(),
							oneRecord.getChannelCode(), oneRecord.getProductID(), 
							rule, oneRecord.getProvinceId(), oneRecord.getOrderType(),
							oneRecord.getBookID()));
		}
	}

	@Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamId.DATASTREAM2.name(),
                new Fields(FName.MSISDN.name(), FName.SESSIONID.name(), FName.RECORDTIME.name(),
                        FName.REALINFORFEE.name(), FName.CHANNELCODE.name(), FName.PRODUCTID.name(),
                        FName.PROVINCEID.name(), FName.ORDERTYPE.name(), FName.BOOKID.name()));

        declarer.declareStream(StreamId.ABNORMALDATASTREAM2.name(),
                new Fields(FName.MSISDN.name(), FName.SESSIONID.name(), FName.RECORDTIME.name(),
                        FName.REALINFORFEE.name(), FName.CHANNELCODE.name(), FName.PRODUCTID.name(),
                        FName.RULES.name(), FName.PROVINCEID.name(), FName.ORDERTYPE.name(),
                        FName.BOOKID.name()));
    }
}
