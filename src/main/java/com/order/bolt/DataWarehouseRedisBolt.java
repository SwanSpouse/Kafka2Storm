package com.order.bolt;

/**
 * Created by LiMingji on 15/7/13.
 */

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.order.db.RedisBoltDBHelper.DBDataWarehouseBoltRedisHelper;
import com.order.db.RedisBoltDBHelper.DBRedisHelper.OrderInRedisHelper;
import com.order.util.FName;
import com.order.util.RuleUtil;
import com.order.util.StreamId;
import com.order.util.TimeParaser;

import java.util.List;

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
 */
public class DataWarehouseRedisBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 1L;
    private DBDataWarehouseBoltRedisHelper dbHelper = new DBDataWarehouseBoltRedisHelper();
    private OrderInRedisHelper orderInRedisHelper = new OrderInRedisHelper();

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (input.getSourceStreamId().equals(StreamId.REDISDATASTREAM.name())) {
            handleRedisDataStream(input, collector);
        }
    }

    // 处理异常数据流
    private void handleRedisDataStream(Tuple input, BasicOutputCollector collector) {
        String msisdn = input.getStringByField(FName.MSISDN.name());
        String sessionId = input.getStringByField(FName.SESSIONID.name());
        Long recordTime = input.getLongByField(FName.RECORDTIME.name());
        double realInfoFee = input.getDoubleByField(FName.REALINFORFEE.name());
        String channelCode = input.getStringByField(FName.CHANNELCODE.name());
        String productId = input.getStringByField(FName.PRODUCTID.name());
        String provinceId = input.getStringByField(FName.PROVINCEID.name());
        int orderType = input.getIntegerByField(FName.ORDERTYPE.name());
        String bookId = input.getStringByField(FName.BOOKID.name());
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

        //订单入缓存
        dbHelper.insertDataToCache(recordTime, msisdn, sessionId, channelCode, bookId, productId, realInfoFee, rule);
        //订单入Redis 用于追溯。
        orderInRedisHelper.putOrderInRedis(msisdn, recordTime, realInfoFee, channelCode, provinceId, contentId, contentType, rule);

        //订单发送到RealTimeBolt
        collector.emit(StreamId.REDISREALTIMEDATA.name(),
                new Values(recordTime, realInfoFee, channelCode, provinceId,
                        contentId, contentType, rule));
        /**
         * 实现追溯功能
         * 1 2 3 5 6 7 8 这些规则是向前追溯该渠道下1小时数据。
         4    追溯一天的数据
         9 10 11 追溯自然小时的数据。
         */
        String[] ruleArr = rule.split("\\|");
        for (int i = 0; i < ruleArr.length; i++) {
            if (ruleArr[i].trim().equals("")) {
                continue;
            }
            int ruleId = RuleUtil.getRuleNumFromString(ruleArr[i]);
            if (ruleId == 0) {
                continue;
            }
            Long traceBackTime ;
            if (ruleId == 1 || ruleId == 2 || ruleId == 3 ||
                    ruleId == 5 || ruleId == 6 || ruleId == 7 || ruleId == 8) {
                traceBackTime = TimeParaser.OneHourAgo(recordTime);
            } else if (ruleId == 4) {
                traceBackTime = TimeParaser.OneDayAgo(recordTime);
            } else {
                traceBackTime = TimeParaser.NormalHourAgo(recordTime);
            }

            //追溯。
            List<String> list = orderInRedisHelper.traceOrderFromRedis(channelCode, msisdn, traceBackTime, ruleId);
            for (String order : list) {
                String[] orderFields = order.split("\\|");
//              0 channelCode + "|" + 1 msisdn + "|" + 2 recordTime;
//              3 realInfoFee + "|" + 4 provinceId + "|" + 5 contentId + "|" + 6 contentType + "|" + 7 rulesInRedis;
                collector.emit(StreamId.REDISREALTIMEDATA.name(),
                        new Values(orderFields[2], orderFields[3], orderFields[0], orderFields[4],
                                orderFields[5], orderFields[6], ruleId));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamId.REDISREALTIMEDATA.name(),
                new Fields(FName.RECORDTIME.name(), FName.REALINFORFEE.name(), FName.CHANNELCODE.name(), FName.PROVINCEID.name(),
                        FName.CONTENTID.name(), FName.CONTENTTYPE.name(), FName.RULES.name()));
    }
}

