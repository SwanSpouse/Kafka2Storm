package com.order.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.jcraft.jsch.Session;
import com.order.constant.Constant;
import com.order.databean.SessionInfo;
import com.order.databean.TimeCacheStructures.Pair;
import com.order.databean.TimeCacheStructures.RealTimeCacheList;
import com.order.databean.UserInfo;
import com.order.util.FName;
import com.order.util.StreamId;
import com.order.util.TimeParaser;
import org.apache.log4j.Logger;


/**
 * Created by LiMingji on 2015/5/24.
 */
public class StatisticsBolt extends BaseBasicBolt {
    private static Logger log =Logger.getLogger(StatisticsBolt.class);

    private RealTimeCacheList<Pair<String, UserInfo>> userInfos =
            new RealTimeCacheList<Pair<String, UserInfo>>(Constant.ONE_DAY);

    private RealTimeCacheList<Pair<String, SessionInfo>> sessionInfos =
            new RealTimeCacheList<Pair<String, SessionInfo>>(Constant.ONE_DAY);

    private void constructInfoFromBrowseData(Tuple input) {
        String remoteIp = input.getStringByField(FName.REMOTEIP.name());
        Long recordTime = TimeParaser.splitTime(input.getStringByField(FName.RECORDTIME.name()));
        String sessionId = input.getStringByField(FName.SESSIONID.name());
        String userAgent = input.getStringByField(FName.USERAGENT.name());
        String pageType = input.getStringByField(FName.PAGETYPE.name());
        String msisdn = input.getStringByField(FName.MSISDN.name());
        int channelCode = input.getIntegerByField(FName.CHANNELCODE.name());
        String bookId = input.getStringByField(FName.BOOKID.name());
        String chapterId = input.getStringByField(FName.CHAPTERID.name());

        if (sessionId == null || !sessionId.trim().equals("") ) {
            return ;
        }
        //更新阅读浏览话单的SessionInfos信息
        Pair<String, SessionInfo> sessionPair = new Pair<String, SessionInfo>(sessionId, null);
        if (sessionInfos.contains(sessionPair)) {
            SessionInfo currentSessionInfo = (SessionInfo) sessionInfos.get(sessionPair).getValue();
            currentSessionInfo.upDateSeesionInfo(bookId, null, null, recordTime, -1, 0, channelCode, -1);
        } else {
            SessionInfo currentSessionInfo = new SessionInfo(sessionId, msisdn, bookId, null, null, recordTime, -1, 0, channelCode, -1);
            sessionInfos.put(new Pair<String, SessionInfo>(sessionId, currentSessionInfo));
        }

        //更新阅读浏览话单的UserInfos信息
        Pair<String, UserInfo> userPair = new Pair<String, UserInfo>(msisdn, null);
        if (userInfos.contains(userPair)) {
            UserInfo currentUserInfo = (UserInfo) userInfos.get(userPair).getValue();
            currentUserInfo.upDateUserInfo(recordTime, sessionId, remoteIp, userAgent);
        } else {
            UserInfo currentUserInfo = new UserInfo(msisdn, recordTime, sessionId, remoteIp, userAgent);
            userInfos.put(new Pair<String, UserInfo>(msisdn, currentUserInfo));
        }
    }

    private void constructInfoFromOrderData(Tuple input) {
        String msisdn = input.getStringByField(FName.MSISDN.name());
        Long recordTime = TimeParaser.splitTime(input.getStringByField(FName.RECORDTIME.name()));
        String userAgent = input.getStringByField(FName.USERAGENT.name());
        int orderType = input.getIntegerByField(FName.ORDERTYPE.name());
        String productId = input.getStringByField(FName.PRODUCTID.name());
        String bookId = input.getStringByField(FName.BOOKID.name());
        String chapterId = input.getStringByField(FName.CHAPTERID.name());
        int channelCode = input.getIntegerByField(FName.CHANNELCODE.name());
        int realInfoFee = input.getIntegerByField(FName.REALINFORFEE.name());
        String wapIp = input.getStringByField(FName.WAPIP.name());
        String sessionId = input.getStringByField(FName.SESSIONID.name());
        int promotionId = input.getIntegerByField(FName.PROMOTIONID.name());
        if (sessionId == null || sessionId.trim().equals("")) {
            return;
        }
        //TODO 在这里开始判断各种规则。
        //更新订购话单的SessionInfos信息
        Pair<String, SessionInfo> sessionInfoPair = new Pair<String, SessionInfo>(sessionId, null);
        if (sessionInfos.contains(sessionInfoPair)) {
            SessionInfo currentSessionInfo = (SessionInfo) sessionInfos.get(sessionInfoPair).getValue();
            currentSessionInfo.upDateSeesionInfo(null, bookId, chapterId, recordTime, orderType,
                    realInfoFee, channelCode, promotionId);
        } else {
            SessionInfo currentSessionInfo = new SessionInfo(sessionId, msisdn, null, bookId,
                    chapterId, recordTime, orderType, realInfoFee, channelCode, promotionId);
            sessionInfos.put(new Pair<String, SessionInfo>(sessionId, currentSessionInfo));
        }
        //更新订购话单UserInfos信息
        Pair<String, UserInfo> userInfoPair = new Pair<String, UserInfo>(msisdn, null);
        if (userInfos.contains(userInfoPair)) {
            UserInfo userInfo = (UserInfo) userInfos.get(userInfoPair).getValue();
            userInfo.upDateUserInfo(recordTime, sessionId, wapIp, userAgent);
        } else {
            UserInfo currentUserInfo = new UserInfo(msisdn, recordTime, sessionId, wapIp, userAgent);
            userInfos.put(new Pair<String, UserInfo>(msisdn, currentUserInfo));
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (input.getSourceStreamId().equals(StreamId.BROWSEDATA)) {
            //阅读浏览话单
            this.constructInfoFromBrowseData(input);
        }else if (input.getSourceStreamId().equals(StreamId.ORDERDATA)) {
            // 订购话单
            this.constructInfoFromOrderData(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
