package com.order.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.order.constant.Constant;
import com.order.constant.Rules;
import com.order.databean.RulesCallback.EmitDatas;
import com.order.databean.SessionInfo;
import com.order.databean.TimeCacheStructures.Pair;
import com.order.databean.TimeCacheStructures.RealTimeCacheList;
import com.order.databean.UserInfo;
import com.order.databean.cleaner.SessionInfoCleaner;
import com.order.databean.cleaner.UserInfoCleaner;
import com.order.db.DBHelper.DBStatisticBoltHelper;
import com.order.util.FName;
import com.order.util.StreamId;
import com.order.util.TimeParaser;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.Map;

/**
 * Created by LiMingji on 2015/5/24.
 */
public class StatisticsBolt extends BaseBasicBolt {
    public static boolean isDebug = false;
    private static Logger log = Logger.getLogger(StatisticsBolt.class);

    //存储字段为msisdn 和 UserInfo
    public  RealTimeCacheList<Pair<String, UserInfo>> userInfos =
            new RealTimeCacheList<Pair<String, UserInfo>>(Constant.ONE_HOUR);
    private UserInfoCleaner userInfoCleaner = null;

    //存储字段为msisdn 和 SessionInfo
    public  RealTimeCacheList<Pair<String, SessionInfo>> sessionInfos =
            new RealTimeCacheList<Pair<String, SessionInfo>>(Constant.ONE_DAY);
    private SessionInfoCleaner sessionInfoCleaner = null;

    //负责每天导入维表的数据
    private transient Thread loader = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    	super.prepare(stormConf, context);
    	try {
			DBStatisticBoltHelper.getData();
		} catch (SQLException e) {
			e.printStackTrace();
		}
    }
    
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
       if (loader == null) {
            //启动线程每天3点准时load数据
            loader = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            long sleepTime = TimeParaser.getMillisFromNowToThreeOclock();
                            if (sleepTime > 0) {
                                loader.sleep(sleepTime);
                            }
                            DBStatisticBoltHelper.getData();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            loader.setDaemon(true);
            loader.start();
        }
        if (userInfoCleaner == null) {
            userInfoCleaner = new UserInfoCleaner(this);
            userInfoCleaner.setDaemon(true);
            userInfoCleaner.start();
        }
        if (sessionInfoCleaner == null) {
            sessionInfoCleaner = new SessionInfoCleaner(this);
            sessionInfoCleaner.setDaemon(true);
            sessionInfoCleaner.start();
        }
        if (input.getSourceStreamId().equals(StreamId.BROWSEDATA.name())) {
            //阅读浏览话单
            try {
                this.constructInfoFromBrowseData(input);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (input.getSourceStreamId().equals(StreamId.ORDERDATA.name())) {
            // 订购话单
            try {
                this.constructInfoFromOrderData(input, collector);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void constructInfoFromBrowseData(Tuple input) throws NumberFormatException {
        Long recordTime = input.getLongByField(FName.RECORDTIME.name());
        String sessionId = input.getStringByField(FName.SESSIONID.name());
        String pageType = input.getStringByField(FName.PAGETYPE.name());
        String msisdn = input.getStringByField(FName.MSISDN.name());
        String channelCode = input.getStringByField(FName.CHANNELCODE.name());
        String bookId = input.getStringByField(FName.BOOKID.name());
        String chapterId = input.getStringByField(FName.CHAPTERID.name());

        if (sessionId == null || sessionId.trim().equals("")) {
            //浏览话单若无sessionId则直接丢弃。
            return;
        }

        //更新阅读浏览话单的SessionInfos信息
        Pair<String, SessionInfo> sessionPair = new Pair<String, SessionInfo>(msisdn, null);
        if (sessionInfos.contains(sessionPair)) {
            SessionInfo currentSessionInfo = (SessionInfo) sessionInfos.get(sessionPair).getValue();
            currentSessionInfo.updateSessionInfo(sessionId, bookId, null, null, recordTime, -1, 0.0, channelCode, null, "");
            sessionInfos.put(new Pair<String, SessionInfo>(msisdn, currentSessionInfo));
        } else {
            SessionInfo currentSessionInfo = new SessionInfo(sessionId, msisdn, bookId, null, null, recordTime, -1, 0, channelCode, null, "");
            sessionInfos.put(new Pair<String, SessionInfo>(msisdn, currentSessionInfo));
        }
        //浏览话单不需要更新用户信息
    }

    private void constructInfoFromOrderData(Tuple input, final BasicOutputCollector collector) throws NumberFormatException {
        String msisdn = input.getStringByField(FName.MSISDN.name());
        Long recordTime = TimeParaser.splitTime(input.getStringByField(FName.RECORDTIME.name()));
        String userAgent = input.getStringByField(FName.TERMINAL.name());
        String platform = input.getStringByField(FName.PLATFORM.name());
        String orderTypeStr = input.getStringByField(FName.ORDERTYPE.name());
        String productId = input.getStringByField(FName.PRODUCTID.name());
        String bookId = input.getStringByField(FName.BOOKID.name());
        String chapterId = input.getStringByField(FName.CHAPTERID.name());
        String channelCode = input.getStringByField(FName.CHANNELCODE.name());
        String realInfoFeeStr = input.getStringByField(FName.COST.name());
        String provinceId = input.getStringByField(FName.PROVINCEID.name());
        String wapIp = input.getStringByField(FName.WAPIP.name());
        String sessionId = input.getStringByField(FName.SESSIONID.name());

        int orderType = Integer.parseInt(orderTypeStr);
        double realInfoFee = Double.parseDouble(realInfoFeeStr);

        //如果无sessionId 则将sessionId设置为NULL。
        if (sessionId == null || sessionId.trim().equals("")) {
            sessionId = "NULL";
        }

        //所有订单数据先统一发送正常数据流。用作数据统计。
        collector.emit(StreamId.DATASTREAM.name(), new Values(msisdn, sessionId, recordTime,
                realInfoFee, channelCode, productId, provinceId, orderType, bookId));
        //更新订购话单的SessionInfos信息
        Pair<String, SessionInfo> sessionInfoPair = new Pair<String, SessionInfo>(msisdn, null);
        SessionInfo currentSessionInfo;
        if (sessionInfos.contains(sessionInfoPair)) {
            currentSessionInfo = (SessionInfo) sessionInfos.get(sessionInfoPair).getValue();
            currentSessionInfo.updateSessionInfo(sessionId, null, bookId, chapterId, recordTime, orderType,
                    realInfoFee, channelCode, productId, provinceId);
            sessionInfos.put(new Pair<String, SessionInfo>(msisdn, currentSessionInfo));
        } else {
            currentSessionInfo = new SessionInfo(sessionId, msisdn, null, bookId,
                    chapterId, recordTime, orderType, realInfoFee, channelCode, productId, provinceId);
            sessionInfos.put(new Pair<String, SessionInfo>(msisdn, currentSessionInfo));
        }
        
        //检测相应的各个规则。
        if (!sessionId.equals("NULL")) {
            currentSessionInfo.checkRule123(bookId, new EmitDatas(collector));
            currentSessionInfo.checkRule6(new EmitDatas(collector));
            currentSessionInfo.checkRule7(new EmitDatas(collector));
            currentSessionInfo.checkRule8(bookId, new EmitDatas(collector));
            currentSessionInfo.checkRule12(platform, new EmitDatas(collector));
        }
        currentSessionInfo.checkRule4(new EmitDatas(collector));
        currentSessionInfo.checkRule5(channelCode, new EmitDatas(collector));

        //更新订购话单UserInfos信息
        Pair<String, UserInfo> userInfoPair = new Pair<String, UserInfo>(msisdn, null);
        UserInfo currentUserInfo;
        if (userInfos.contains(userInfoPair)) {
            currentUserInfo = (UserInfo) userInfos.get(userInfoPair).getValue();
            currentUserInfo.upDateUserInfo(recordTime, sessionId, wapIp, userAgent);
            userInfos.put(new Pair<String, UserInfo>(msisdn, currentUserInfo));
        } else {
            currentUserInfo = new UserInfo(msisdn, recordTime, sessionId, wapIp, userAgent);
            userInfos.put(new Pair<String, UserInfo>(msisdn, currentUserInfo));
        }

        boolean[] isObeyRules = currentUserInfo.isObeyRules();
        if (!isObeyRules[UserInfo.SESSION_CHECK_BIT]) {
            collector.emit(StreamId.ABNORMALDATASTREAM.name(),
                    new Values(msisdn, sessionId, recordTime, realInfoFee, channelCode, productId,
                            provinceId, orderType, bookId, Rules.NINE.name()));
        }
        
        if (!isObeyRules[UserInfo.IP_CHECK_BIT]) {
            collector.emit(StreamId.ABNORMALDATASTREAM.name(),
                    new Values(msisdn, sessionId, recordTime, realInfoFee, channelCode, productId,
                            provinceId, orderType, bookId, Rules.TEN.name()));
        }

        if (!isObeyRules[UserInfo.UA_CHECK_BIT]) {
            collector.emit(StreamId.ABNORMALDATASTREAM.name(),
                    new Values(msisdn, sessionId, recordTime, realInfoFee, channelCode, productId,
                            provinceId, orderType, bookId, Rules.ELEVEN.name()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamId.DATASTREAM.name(),
                new Fields(FName.MSISDN.name(), FName.SESSIONID.name(), FName.RECORDTIME.name(),
                        FName.REALINFORFEE.name(), FName.CHANNELCODE.name(), FName.PRODUCTID.name(),
                        FName.PROVINCEID.name(), FName.ORDERTYPE.name(), FName.BOOKID.name()));

        declarer.declareStream(StreamId.ABNORMALDATASTREAM.name(),
                new Fields(FName.MSISDN.name(), FName.SESSIONID.name(), FName.RECORDTIME.name(),
                        FName.REALINFORFEE.name(), FName.CHANNELCODE.name(), FName.PRODUCTID.name(),
                        FName.PROVINCEID.name(), FName.ORDERTYPE.name(), FName.BOOKID.name(),
                        FName.RULES.name()));

    }
}
