package com.order.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.order.db.DBHelper.DBDataWarehouseCacheHelper;
import com.order.util.FName;
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
        } else if (input.getSourceStreamId().equals(StreamId.ABNORMALDATASTREAM.name())) {
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

        /**
         *  根据 orderType & productId & bookId 生成contentType 和 contentId
         * */
        String contentId;
        String contentType;
        if (orderType == 4) { // 包月
            contentType = 1 + "";
            contentId = productId;
        } else if (orderType == 5) { // 促销包
            contentType = 2 + "";
            contentId = productId;
        } else { // 图书
            contentType = 3 + "";
            contentId = bookId;
        }

        // 订购记录增加到内存
        if (DBHelper.insertData(msisdn, sessionId, channelCode, recordTime, bookId,
                productId, realInfoFee, provinceId, orderType) == 1) {
            // 若新增成功则直接转发消息
            collector.emit(StreamId.DATASTREAM2.name(), new Values(msisdn, sessionId, recordTime,
                    realInfoFee, channelCode, provinceId, contentId, contentType));
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

        /**
         *  根据 orderType & productId & bookId 生成contentType 和 contentId
         * */
        String contentId;
        String contentType;
        if (orderType == 4) { // 包月
            contentType = 1 + "";
            contentId = productId;
        } else if (orderType == 5) { // 促销包
            contentType = 2 + "";
            contentId = productId;
        } else { // 图书
            contentType = 3 + "";
            contentId = bookId;
        }

        // 如果update时找不到订购记录，则首先插入一条
        int result = DBHelper.updateData(msisdn, sessionId, channelCode, recordTime, bookId,
                productId, realInfoFee, provinceId, orderType, rule);
        if (result == -1) {
        	// 找不到异常订购对应的正常订购，直接返回，不再发新的。
        	return;
        	
            //// 订购记录增加到内存
            //if (DBHelper.insertData(msisdn, sessionId, channelCode, recordTime, bookId,
            //        productId, realInfoFee, provinceId, orderType) == 1) {
            //    // 若新增成功则直接转发正常订购消息
            //    collector.emit(StreamId.DATASTREAM2.name(), new Values(msisdn, sessionId, recordTime,
            //            realInfoFee, channelCode, provinceId, contentId, contentType));
            //    count("send");
            //}
            //// 再次更新
            //result = DBHelper.updateData(msisdn, sessionId, channelCode, recordTime, bookId,
            //        productId, realInfoFee, provinceId, orderType, rule);
        }
        // 更新成功后直接转发异常订购
        if (result == 1) {
            collector.emit(StreamId.ABNORMALDATASTREAM2.name(), new Values(msisdn, sessionId, recordTime,
                    realInfoFee, channelCode, provinceId, rule, contentId, contentType));
        }

        // 向前追溯准备阶段
        int ruleId = DBHelper.getRuleNumFromString(rule);
        if (ruleId < 1 || ruleId > 12) {
            return;
        }
        /**
         * 1 2 3 6 7 8 这些规则是向前追溯该渠道下1小时数据。
         4 5  追溯该渠道一天的数据
         9 10 11 追溯自然小时的所有订购数据（不指定渠道）。
         */
        Long traceBackBeginTime = (long) 0;
        Long traceBackEndTime = (long) 0;
        String traceBackChannelCode;
        if (ruleId == 1 || ruleId == 2 || ruleId == 3 ||
               ruleId == 6 || ruleId == 7 || ruleId == 8) {
        	traceBackBeginTime = recordTime - 60*60*1000L;
        	traceBackEndTime = recordTime;
        	traceBackChannelCode = channelCode;
        } else if (ruleId == 4 || ruleId == 5) {
        	traceBackBeginTime = TimeParaser.splitTime(TimeParaser.OneDayAgo(recordTime));
        	traceBackEndTime = recordTime;
        	traceBackChannelCode = "";
        } else if (ruleId == 9 || ruleId == 10 || ruleId == 11) {
        	traceBackBeginTime = TimeParaser.splitTime(TimeParaser.NormalHourAgo(recordTime));
        	traceBackEndTime = recordTime;
        	traceBackChannelCode = "";
        } else {
            return;
        }

        ArrayList<OrderRecord> list = DBHelper.traceBackOrders(msisdn,
        		traceBackChannelCode, traceBackBeginTime, traceBackEndTime, ruleId);
        // 将回溯的订购发送
        Iterator<OrderRecord> itOrder = list.iterator();
        while (itOrder.hasNext()) {
            OrderRecord oneRecord = itOrder.next();
            /**
             *  根据 orderType & productId & bookId 生成contentType 和 contentId
             * */
            if (oneRecord.getOrderType() == 4) { // 包月
                contentType = 1 + "";
                contentId = oneRecord.getProductID();
            } else if (oneRecord.getOrderType() == 5) { // 促销包
                contentType = 2 + "";
                contentId = oneRecord.getProductID();
            } else { // 图书
                contentType = 3 + "";
                contentId = oneRecord.getBookID();
            }

            collector.emit(
                    StreamId.ABNORMALDATASTREAM2.name(), new Values(oneRecord.getMsisdn(), oneRecord.getSessionId(),
                            oneRecord.getRecordTime(), oneRecord.getRealfee(), oneRecord.getChannelCode(),
                            oneRecord.getProvinceId(), rule, contentId, contentType));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamId.DATASTREAM2.name(),
                new Fields(FName.MSISDN.name(), FName.SESSIONID.name(), FName.RECORDTIME.name(),
                        FName.REALINFORFEE.name(), FName.CHANNELCODE.name(), FName.PROVINCEID.name(),
                        FName.CONTENTID.name(), FName.CONTENTTYPE.name()));

        declarer.declareStream(StreamId.ABNORMALDATASTREAM2.name(),
                new Fields(FName.MSISDN.name(), FName.SESSIONID.name(), FName.RECORDTIME.name(),
                        FName.REALINFORFEE.name(), FName.CHANNELCODE.name(), FName.PROVINCEID.name(),
                        FName.RULES.name(), FName.CONTENTID.name(), FName.CONTENTTYPE.name()));
    }
}
