package com.order.databean;

import com.order.constant.Constant;
import com.order.constant.Rules;
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
    private String msisdnId = null;
    //真实信息费
    private int realInfoFee = -1;
    //渠道id
    private int channelId = -1;
    //营销参数
    private int promotionId = 0;
    private long lastUpdateTime;

    //订单类型
    private int orderType = 0;

    private Thread rule1Checker = null;
    private Thread rule2Checker = null;

    //图书阅读浏览pv，应用于1、2、6、7
    private RealTimeCacheList<String> bookReadPv = new RealTimeCacheList<String>(Constant.SIXTYFIVE_MINUTES);
    //图书购买pv, 应用于规则6
    private RealTimeCacheList<String> bookOrderPv = new RealTimeCacheList<String>(Constant.FIVE_MINUTES);
    //图书章节购买pv，应用于规则7
    private RealTimeCacheList<String> bookChapterOrderPv = new RealTimeCacheList<String>(Constant.FIVE_MINUTES);


    //对应浏览pv 和 订购pv 构建SeesionInfo
    public SessionInfo(String sessionId, String msisdnId, String bookReadId,
                       String bookOrderId, String bookChapterOrderId, Long currentTime,
                       int orderType, int realInfoFee, int channelId, int promotionId) {
        if (currentTime != null) {
            this.lastUpdateTime = currentTime;
        } else {
            this.lastUpdateTime = System.currentTimeMillis();
        }
        if (sessionId == null || msisdnId == null) {
            log.error("sessionId || msisdn should not be null");
            throw new IllegalArgumentException("sessionId || msisdn should not be null");
        }
        this.sessionId = sessionId;
        this.msisdnId = msisdnId;

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


    /**
     * 检测规则 1
     * 规则1：用户5分钟内，对图书有订购行为，且前1小时后5分钟内对该产品的点击pv=0 ordertype not in ( 4 5 9 99)
     *
     * @param bookId
     * @param callback
     */
    public void checkRule1(final String bookId, final RulesCallback callback) {

    }

    /**
     * 检测规则 2
     * 规则2：用户5分钟内，对图书有订购行为，且前1小时后5分钟内对该产品的点击pv=1 ordertype not in ( 4 5 9 99)
     *
     * @param bookId
     * @param callback
     */
    public void checkRule2(final String bookId, final RulesCallback callback) {

    }

    /**
     * 检测规则 3
     * 规则3：用户5分钟内，对图书有订购行为，且前1小时后5分钟内对该产品的点击 1<pv<=5 ordertype not in ( 4 5 9 99)
     * @param bookId
     * @param callback
     */
    public void checkRule3(final String bookId, final RulesCallback callback) {
        rule1Checker = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    //延迟5分钟之后对65分钟内的数据进行检测。
                    rule1Checker.sleep(Constant.FIVE_MINUTES);
                    if (bookChapterOrderPv.sizeById(bookId) <= Constant.READPV_THREASHOLD) {
                        callback.hanleData(msisdnId, sessionId, lastUpdateTime, realInfoFee, channelId, promotionId, Rules.ONE);
                    } else {
                        callback.hanleData(msisdnId, sessionId, lastUpdateTime, realInfoFee, channelId, promotionId, Rules.ONE);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        rule1Checker.start();
        rule1Checker.setDaemon(true);
    }

    /**
     * 检测规则 4
     * 规则5：一个用户日扣费二级渠道>=3个，
     * @param callback
     */
    public void checkRule4(final RulesCallback callback) {
        //TODO 还需要存一个二级渠道的变量。日过期。
    }

    /**
     * 检测规则 5
     * 规则5：一个用户日渠道ID中的按本订购费>10元，该用户异常渠道当天所有信息费为异常
     * @param callback
     */
    public void checkRule5(final RulesCallback callback) {
        //TODO 这个规则还有待再讨论。
    }

    /**
     * 检测规则 6
     * 规则6：用户3分钟内，包月订购>=2次
     *
     * @param bookId
     * @param callback
     */
    public void checkRule6(String bookId, final RulesCallback callback) {
        if (bookOrderPv.sizeWithTimeThreshold(bookId, lastUpdateTime, Constant.THREEO_MINUTES)
                >= Constant.ORDER_BY_MONTH_THRESHOLD) {
            callback.hanleData(msisdnId, sessionId, lastUpdateTime, realInfoFee, channelId, promotionId, Rules.SEVEN);
        } else {
            callback.hanleData(msisdnId, sessionId, lastUpdateTime, realInfoFee, channelId, promotionId, Rules.SEVEN);
        }
    }

    /**
     * 检测规则 7
     * 规则7：用户5分钟内，完本图书订购本书>=2，且对订购图书的pv<=5*本数
     *
     * @param callback
     */
    public void checkRule7(final RulesCallback callback) {
        if (bookOrderPv.size() < 2) {
            callback.hanleData(msisdnId, sessionId, lastUpdateTime, realInfoFee, channelId, promotionId, Rules.EIGHT);
            return ;
        }
        int orderBookNums = bookOrderPv.size();
        int countBookReadpv = 0;
        for (String bookId : bookOrderPv.keySet()) {
            countBookReadpv += bookReadPv.sizeWithTimeThreshold(bookId, lastUpdateTime, Constant.FIVE_MINUTES);
        }
        if (orderBookNums <= 5 * countBookReadpv) {
            callback.hanleData(msisdnId, sessionId, lastUpdateTime, realInfoFee, channelId, promotionId, Rules.EIGHT);
            return ;
        }
        callback.hanleData(msisdnId, sessionId, lastUpdateTime, realInfoFee, channelId, promotionId, Rules.EIGHT);
    }

    /**
     * 检测规则 8
     * 规则8：用户5分钟内，连载图书订购章数>=10，且对订购图书的pv<=2*章数
     * 不满10个。后续判断不触发。
     *
     * @param callback
     */
    public void checkRule8(String id, final RulesCallback callback) {
        //TODO 这里需要知道这本书包含哪些章数。
    }

    public void clean() {
        rule1Checker.interrupt();
        rule2Checker.interrupt();
    }
}