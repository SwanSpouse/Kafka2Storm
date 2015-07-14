package com.order.bolt.Redis;

/**
 * Created by LiMingji on 15/7/13.
 */

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.order.db.RedisBoltDBHelper.DBRealTimeOutputBoltRedisHelper;
import com.order.db.RedisBoltDBHelper.DBRedisHelper.DBTotalFeeRedisHelper;
import com.order.util.FName;
import com.order.util.StreamId;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * 实时输出Bolt
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
 *
 *
 */
public class RealTimeOutputRedisBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 1L;
    private DBTotalFeeRedisHelper DBHelper = null;
    private DBRealTimeOutputBoltRedisHelper realTimeHelper = null;
    private static Logger log = Logger.getLogger(RealTimeOutputRedisBolt.class);

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        if (DBHelper == null) {
            DBHelper = new DBTotalFeeRedisHelper();
        }
        if (realTimeHelper == null) {
            realTimeHelper = new DBRealTimeOutputBoltRedisHelper(DBHelper);
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (DBHelper == null) {
            DBHelper = new DBTotalFeeRedisHelper();
        }
        if (realTimeHelper == null) {
            realTimeHelper = new DBRealTimeOutputBoltRedisHelper(DBHelper);
        }
        // 开始处理消息
        if (input.getSourceStreamId().equals(StreamId.REDISREALTIMEDATA.name())) {
            dealDataStream(input);
        }
    }

    /**
     * 处理数据流
     */
    private void dealDataStream(Tuple input) {
        Long recordTime = input.getLongByField(FName.RECORDTIME.name());
        double realInfoFee = input.getDoubleByField(FName.REALINFORFEE.name());
        String channelCode = input.getStringByField(FName.CHANNELCODE.name());
        String provinceId = input.getStringByField(FName.PROVINCEID.name());
        String contentId = input.getStringByField(FName.CONTENTID.name());
        String contentType = input.getStringByField(FName.CONTENTTYPE.name());
        String rules = input.getStringByField(FName.RULES.name());

        //将总费用统计到Redis中。
        DBHelper.insertFeeToRedis(recordTime,provinceId,channelCode,contentId,contentType,realInfoFee,rules);
        //将待入库的数据放入缓存中。
        String[] ruleArr = rules.split("\\|");
        for (int i = 0; i < ruleArr.length; i++) {
            if (ruleArr[i].trim().equals("")) {
                continue;
            }
            realTimeHelper.insert2Cache(recordTime, provinceId, channelCode, contentId,
                    contentType, ruleArr[i], realInfoFee);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //Do Nothing
    }
}

