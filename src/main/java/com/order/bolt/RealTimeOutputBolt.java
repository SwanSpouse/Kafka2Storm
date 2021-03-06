package com.order.bolt;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.order.db.DBHelper.DBRealTimeOutputBoltHelper;
import com.order.util.FName;
import com.order.util.StreamId;

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
 * <p/>
 * Created by LiMingji on 2015/5/24.
 */
public class RealTimeOutputBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	private DBRealTimeOutputBoltHelper DBHelper = null;
    private static Logger log = Logger.getLogger(RealTimeOutputBolt.class);
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        if (DBHelper == null) {
            DBHelper = new DBRealTimeOutputBoltHelper();
        }
    }
    
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (DBHelper == null) {
            DBHelper = new DBRealTimeOutputBoltHelper();
        }
        // 检查是否清除要清除内存到数据库  --测试发现Capacity指标太大，看不到负荷情况
        //DBHelper.checkClear();
        // 开始处理消息
        if (input.getSourceStreamId().equals(StreamId.DATASTREAM2.name())) {
            //从DataWarehouse发来的正常统计数据流
            dealNormalData(input);
        } else if (input.getSourceStreamId().equals(StreamId.ABNORMALDATASTREAM2.name())) {
            //从DataWarehouse发来的异常订购数据流
            dealAbnormalData(input);
        }
    }

    /**
     * 处理正常数据流
     */
    private void dealNormalData(Tuple input) {
        String msisdn = input.getStringByField(FName.MSISDN.name());
        Long recordTime = input.getLongByField(FName.RECORDTIME.name());
        double realInfoFee = input.getDoubleByField(FName.REALINFORFEE.name());
        String channelCode = input.getStringByField(FName.CHANNELCODE.name());
        String provinceId = input.getStringByField(FName.PROVINCEID.name());
        String contentId = input.getStringByField(FName.CONTENTID.name());
        String contentType = input.getStringByField(FName.CONTENTTYPE.name());

        DBHelper.updateData(msisdn, recordTime, channelCode, provinceId, "0", realInfoFee, contentId, contentType);
    }

    /**
     * 处理异常数据流
     */
    private void dealAbnormalData(Tuple input) {
        String msisdn = input.getStringByField(FName.MSISDN.name());
        Long recordTime = input.getLongByField(FName.RECORDTIME.name());
        double realInfoFee = input.getDoubleByField(FName.REALINFORFEE.name());
        String channelCode = input.getStringByField(FName.CHANNELCODE.name());
        String rule = input.getStringByField(FName.RULES.name());
        String provinceId = input.getStringByField(FName.PROVINCEID.name());
        String contentId = input.getStringByField(FName.CONTENTID.name());
        String contentType = input.getStringByField(FName.CONTENTTYPE.name());

        DBHelper.updateData(msisdn, recordTime, channelCode, provinceId, rule, realInfoFee, contentId, contentType);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //Do Nothing
    }
}
