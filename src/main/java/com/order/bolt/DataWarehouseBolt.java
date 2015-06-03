package com.order.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.order.util.FName;
import com.order.util.StreamId;

/**
 * 数据仓库Bolt
 *
 * 输出表结构
 * CREATE TABLE "AAS"."ABN_CTID_CTTP_PARM_PRV_D"
 * (
 * "RECORD_DAY"   VARCHAR2(8 BYTE),
 * "PROVINCE_ID"  VARCHAR2(32 BYTE),
 * "CHL1"         VARCHAR2(40 BYTE),
 * "CHL2"         VARCHAR2(40 BYTE),
 * "CHL3"         VARCHAR2(40 BYTE),
 * "CONTENT_ID"   VARCHAR2(19 BYTE),
 * "SALE_PARM"    VARCHAR2(62 BYTE),
 * "ODR_ABN_FEE"  NUMBER,
 * "ODR_FEE"      NUMBER,
 * "ABN_RAT"      NUMBER,
 * "ODR_CPL_CNT"  NUMBER,
 * "ODR_CNT"      NUMBER,
 * "CPL_RAT"      NUMBER,
 * "CONTENT_TYPE" VARCHAR2(4 BYTE),
 * "RULE_ID"      NUMBER,
 * "CPL_INCOME_RAT"  NUMBER
 * )
 * <p/>
 * Created by LiMingji on 2015/5/24.
 */
public class DataWarehouseBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (input.getSourceStreamId().equals(StreamId.DATASTREAM.name())) {
            //正常统计数据流
            dealNormalDate(input);
        } else if (input.getSourceStreamId().equals(StreamId.ABNORMALDATASTREAM)) {
            //异常订购数据流
            dealAbnormalData(input);
        }
    }

    /**
     * 处理正常数据流
     */
    private void dealNormalDate(Tuple input) {
//        collector.emit(StreamId.DATASTREAM.name(), new Values(msisdn, sessionId, recordTime,realInfoFee,
//        channelCode, promotionId, provinceId));
        String msisdn = input.getStringByField(FName.MSISDN.name());
        String seesionId = input.getStringByField(FName.SESSIONID.name());
        Long recordTime = input.getLongByField(FName.RECORDTIME.name());
        int realInfoFee = input.getIntegerByField(FName.REALINFORFEE.name());
        int channelCode = input.getIntegerByField(FName.CHANNELCODE.name());
        int promotionId = input.getIntegerByField(FName.PROMOTIONID.name());
        int provinceId = input.getIntegerByField(FName.PROVINCEID.name());
    }

    /**
     * 处理异常数据流
     */
    private void dealAbnormalData(Tuple input) {
//        String msisdnId, String sessionId, Long currentTime, int realInfoFee,
//                          int channelId, int promotionId, Rules rules, int provinceId
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
