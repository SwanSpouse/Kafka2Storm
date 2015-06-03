package com.order.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.order.util.FName;
import com.order.util.StreamId;


/**
 * 实时输出Bolt，计算异常率，定时写入数据库
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
public class RealtimeOuputBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (input.getSourceStreamId().equals(StreamId.DATASTREAM)) {
            handleDataStream(input);
        }else if (input.getSourceStreamId().equals(StreamId.ABNORMALDATASTREAM)) {
            handleAbnormalDataStream(input);
        }
    }

    //处理正常数据流
    private void handleDataStream(Tuple input) {
        //collector.emit(StreamId.DATASTREAM.name(), new Values(msisdn, sessionId, recordTime,realInfoFee,
        // channelCode, promotionId, provinceId));
        String msisdn = input.getStringByField(FName.MSISDN.name());
        String seesionId = input.getStringByField(FName.SESSIONID.name());
        Long recordTime = input.getLongByField(FName.RECORDTIME.name());
        int realInfoFee = input.getIntegerByField(FName.REALINFORFEE.name());
        int channelCode = input.getIntegerByField(FName.CHANNELCODE.name());
        int promotionId = input.getIntegerByField(FName.PROMOTIONID.name());
        int provinceId = input.getIntegerByField(FName.PROVINCEID.name());
    }

    //处理异常数据流
    private void handleAbnormalDataStream(Tuple input) {
        String msisdn = input.getStringByField(FName.MSISDN.name());
        String sessionId = input.getStringByField(FName.SESSIONID.name());
        Long recordTime = input.getLongByField(FName.RECORDTIME.name());
        int realInfoFee = input.getIntegerByField(FName.REALINFORFEE.name());
        int channelCode = input.getIntegerByField(FName.CHANNELCODE.name());
        int promotionId = input.getIntegerByField(FName.PROMOTIONID.name());
        String rule = input.getStringByField(FName.RULES.name());
        int provinceId = input.getIntegerByField(FName.PROVINCEID.name());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //Do Nothing
    }
}
