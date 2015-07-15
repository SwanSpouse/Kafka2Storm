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
import com.order.util.FName;
import com.order.util.StreamId;

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

        //TODO 这里应该有追溯。从数据库和内存中将追溯的数据再发出去。
        dbHelper.insertDataToCache(recordTime, msisdn, sessionId, channelCode, bookId, productId, realInfoFee, rule);

        collector.emit(StreamId.REDISREALTIMEDATA.name(),
                new Values(recordTime, realInfoFee, channelCode, provinceId,
                        contentId, contentType, rule));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamId.REDISREALTIMEDATA.name(),
                new Fields(FName.RECORDTIME.name(), FName.REALINFORFEE.name(), FName.CHANNELCODE.name(), FName.PROVINCEID.name(),
                        FName.CONTENTID.name(), FName.CONTENTTYPE.name(), FName.RULES.name()));
    }
}

