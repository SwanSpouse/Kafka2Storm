package com.order.databean;

import com.order.constant.Constant;
import com.order.databean.RulesCallback.RulesCallback;
import com.order.databean.TimeCacheStructures.RealTimeCacheList;
import org.apache.log4j.Logger;

/**
 * 根据浏览pv和订购pv生成SessionInfo。
 *
 * Created by LiMingji on 2015/5/21.
 */
public class SessionInfo {
    private static Logger log = Logger.getLogger(SessionInfo.class);
    //SessionId
    private String sessionId = null;
    //包含此Session的用户id
    private String msisdn = null;
    //真实信息费
    private int realInfoFee =-1;
    //渠道id
    private int channelId = -1;
    //营销参数
    private int promotionId = 0;
    private long lastUpdateTime;

    //订单类型
    private int orderType = 0;

    private Thread rules12Checker = null;

    //图书阅读浏览pv，应用于1、2、6、7
    private RealTimeCacheList<String> bookReadPv = new RealTimeCacheList<String>(Constant.SIXTYFIVE_MINUTES);
    //图书购买pv, 应用于规则6
    private RealTimeCacheList<String> bookOrderPv = new RealTimeCacheList<String>(Constant.SIXTYFIVE_MINUTES);
    //图书章节购买pv，应用于规则7
    private RealTimeCacheList<String> bookChapterOrderPv = new RealTimeCacheList<String>(Constant.FIVE_MINUTES);

    //对应浏览pv 和 订购pv 构建SeesionInfo
    public SessionInfo(String sessionId, String msisdn, String bookReadId,
                       String bookOrderId, String bookChapterOrderId, Long currentTime ,
                       int orderType, int realInfoFee, int channelId, int promotionId) {
        if (currentTime != null) {
            this.lastUpdateTime = currentTime;
        } else {
            this.lastUpdateTime = System.currentTimeMillis();
        }
        if (sessionId == null || msisdn == null) {
            log.error("sessionId || msisdn should not be null");
            throw new IllegalArgumentException("sessionId || msisdn should not be null");
        }
        this.sessionId = sessionId;
        this.msisdn = msisdn;

        this.orderType = orderType;
        this.realInfoFee = realInfoFee;
        this.channelId = channelId;
        this.promotionId = promotionId;

        if (bookReadId != null) {
            bookReadPv.put(bookReadId, lastUpdateTime);
        }
        if (bookOrderId != null) {
            bookOrderPv.put(bookOrderId, lastUpdateTime);
        }
        if (bookChapterOrderId != null) {
            bookChapterOrderPv.put(bookChapterOrderId, lastUpdateTime);
        }
    }

    //对已存在的SessionInfo进行更新。
    public void upDateSeesionInfo(String bookReadId, String bookOrderId, String bookChapterOrderId,
                                  Long currentTime, int orderType, int realInfoFee,
                                  int channelId, int promotionId) {
        if (currentTime != null) {
            lastUpdateTime = currentTime;
        } else {
            lastUpdateTime = System.currentTimeMillis();
        }
        if (bookReadId != null) {
            bookReadPv.put(bookReadId, lastUpdateTime);
        }
        if (bookOrderId != null) {
            bookOrderPv.put(bookOrderId, lastUpdateTime);
        }
        if (bookChapterOrderPv != null) {
            bookChapterOrderPv.put(bookChapterOrderId, lastUpdateTime);
        }
        this.orderType = orderType;
        this.realInfoFee = realInfoFee;
        this.channelId = channelId;
        this.promotionId = promotionId;
    }

    //检测规则1、2
    public void checkRules12(String bookId, final RulesCallback callback) {
        rules12Checker = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    rules12Checker.sleep(Constant.FIVE_MINUTES);
                    if (bookChapterOrderPv.size() <= Constant.READPV_THREASHOLD) {
                        if (orderType == Constant.ORDERTYPE_BOOK) {
                            callback.hanleData(msisdn, sessionId, lastUpdateTime, realInfoFee, channelId, promotionId, 1, true);
                        }else if (orderType == Constant.ORDERTYPE_PROMOTION) {
                            callback.hanleData(msisdn, sessionId, lastUpdateTime, realInfoFee, channelId, promotionId, 2, true);
                        }
                    } else {
                        if (orderType == Constant.ORDERTYPE_BOOK) {
                            callback.hanleData(msisdn, sessionId, lastUpdateTime, realInfoFee, channelId, promotionId, 1, false);
                        }else if (orderType == Constant.ORDERTYPE_PROMOTION) {
                            callback.hanleData(msisdn, sessionId, lastUpdateTime, realInfoFee, channelId, promotionId, 2, false);
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        rules12Checker.start();
        rules12Checker.setDaemon(true);
    }
}