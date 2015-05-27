package com.order.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.order.constant.Constant;
import com.order.constant.Rules;
import com.order.databean.RulesCallback.EmitDatas;
import com.order.databean.RulesCallback.RulesCallback;
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

    private void constructInfoFromBrowseData(Tuple input) throws Exception{
        Long recordTime = TimeParaser.splitTime(input.getStringByField(FName.RECORDTIME.name()));
        String sessionId = input.getStringByField(FName.SESSIONID.name());
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
        //浏览话单不需要更新用户信息
    }

    private void constructInfoFromOrderData(Tuple input, final BasicOutputCollector collector) throws Exception {
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
        //所有订单数据先统一发送。用作数据统计。
        collector.emit(StreamId.DATASTREAM.name(), new Values(msisdn, sessionId, recordTime,
                realInfoFee, channelCode, promotionId));

        //更新订购话单的SessionInfos信息
        Pair<String, SessionInfo> sessionInfoPair = new Pair<String, SessionInfo>(sessionId, null);
        SessionInfo currentSessionInfo = null;
        if (sessionInfos.contains(sessionInfoPair)) {
            currentSessionInfo = (SessionInfo) sessionInfos.get(sessionInfoPair).getValue();
            currentSessionInfo.upDateSeesionInfo(null, bookId, chapterId, recordTime, orderType,
                    realInfoFee, channelCode, promotionId);
        } else {
            currentSessionInfo = new SessionInfo(sessionId, msisdn, null, bookId,
                    chapterId, recordTime, orderType, realInfoFee, channelCode, promotionId);
            sessionInfos.put(new Pair<String, SessionInfo>(sessionId, currentSessionInfo));
        }
        //检测相应的各个规则。
        currentSessionInfo.checkRule123(bookId, new EmitDatas(collector));
        currentSessionInfo.checkRule4(new EmitDatas(collector));
        currentSessionInfo.checkRule5(new EmitDatas(collector));
        currentSessionInfo.checkRule6(bookId, new EmitDatas(collector));
        currentSessionInfo.checkRule7(new EmitDatas(collector));
        currentSessionInfo.checkRule8(bookId, new EmitDatas(collector));

        //更新订购话单UserInfos信息
        Pair<String, UserInfo> userInfoPair = new Pair<String, UserInfo>(msisdn, null);
        UserInfo currentUserInfo = null;
        if (userInfos.contains(userInfoPair)) {
            currentUserInfo = (UserInfo) userInfos.get(userInfoPair).getValue();
            currentUserInfo.upDateUserInfo(recordTime, sessionId, wapIp, userAgent);
        } else {
            currentUserInfo = new UserInfo(msisdn, recordTime, sessionId, wapIp, userAgent);
            userInfos.put(new Pair<String, UserInfo>(msisdn, currentUserInfo));
        }
        boolean[] isObeyRules = currentUserInfo.isObeyRules();
        if (!isObeyRules[UserInfo.SESSION_CHECK_BIT]) {
            collector.emit(StreamId.ABNORMALDATASTREAM.name(),
                    new Values(msisdn, sessionId, recordTime, realInfoFee, channelCode, promotionId, Rules.NINE));
        }
        if (!isObeyRules[UserInfo.IP_CHECK_BIT]) {
            collector.emit(StreamId.ABNORMALDATASTREAM.name(),
                    new Values(msisdn, sessionId, recordTime, realInfoFee, channelCode, promotionId, Rules.TEN));
        }
        if (!isObeyRules[UserInfo.UA_CHECK_BIT]) {
            collector.emit(StreamId.ABNORMALDATASTREAM.name(),
                    new Values(msisdn, sessionId, recordTime, realInfoFee, channelCode, promotionId, Rules.ELEVEN));
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (input.getSourceStreamId().equals(StreamId.BROWSEDATA)) {
            //阅读浏览话单
            try {
                this.constructInfoFromBrowseData(input);
            } catch (Exception e) {
                log.error("阅读浏览话单数据结构异常");
            }
        }else if (input.getSourceStreamId().equals(StreamId.ORDERDATA)) {
            // 订购话单
            try {
                this.constructInfoFromOrderData(input, collector);
            } catch (Exception e) {
                log.error("订购话单数据结构异常");
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamId.DATASTREAM.name(),
                new Fields(FName.MSISDN.name(), FName.SESSIONID.name(), FName.RECORDTIME.name(),
                           FName.REALINFORFEE.name(), FName.CHANNELCODE.name(), FName.PROMOTIONID.name()));

        declarer.declareStream(StreamId.ABNORMALDATASTREAM.name(),
                new Fields(FName.MSISDN.name(), FName.SESSIONID.name(), FName.RECORDTIME.name(),
                            FName.REALINFORFEE.name(), FName.CHANNELCODE.name(), FName.PROMOTIONID.name(),
                            FName.RULES.name()));
    }
}
