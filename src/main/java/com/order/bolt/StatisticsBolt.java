package com.order.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.order.constant.Constant;
import com.order.databean.SessionInfo;
import com.order.databean.TimeCacheStructures.RealTimeCacheList;
import com.order.databean.UserInfo;
import com.order.util.FName;
import com.order.util.StreamId;
import org.apache.log4j.Logger;

import java.util.HashMap;


/**
 * Created by LiMingji on 2015/5/24.
 */
public class StatisticsBolt extends BaseBasicBolt {
    private static Logger log =Logger.getLogger(StatisticsBolt.class);

    //TODO 这里应该也是一个定时清理的map
    private HashMap<String, UserInfo> userInfos = new HashMap<String, UserInfo>();
    private HashMap<String, SessionInfo> sessionInfos = new HashMap<String, SessionInfo>();

    private UserInfo constructUserInfo(Tuple input) {
        String remoteIp = input.getStringByField(FName.REMOTEIP.name());
        String recordTime = input.getStringByField(FName.RECORDTIME.name());
        String sessionId = input.getStringByField(FName.SESSIONID.name());
        String userAgent = input.getStringByField(FName.USERAGENT.name());
        String pageType = input.getStringByField(FName.PAGETYPE.name());
        String msisdn = input.getStringByField(FName.MSISDN.name());
        String channelCode = input.getStringByField(FName.CHANNELCODE.name());
        String bookId = input.getStringByField(FName.BOOKID.name());
        String chapterId = input.getStringByField(FName.CHAPTERID.name());

        if (sessionId == null || !sessionId.trim().equals("") ) {
            return null;
        }
        return null;
    }

    private SessionInfo constructSessionInfo(Tuple input) {
        return null;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (input.getSourceStreamId().equals(StreamId.BROWSEDATA)) {
            // 浏览话单
            UserInfo currentUserInfo = constructUserInfo(input);
        }else if (input.getSourceStreamId().equals(StreamId.ORDERDATA)) {
            // 订购话单

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
