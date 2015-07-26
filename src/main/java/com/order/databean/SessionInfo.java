package com.order.databean;

import com.order.constant.Constant;
import com.order.constant.Rules;
import com.order.databean.RulesCallback.RulesCallback;
import com.order.databean.TimeCacheStructures.BookOrderList;
import com.order.databean.TimeCacheStructures.CachedList;
import com.order.db.DBHelper.DBStatisticBoltHelper;
import com.order.util.TimeParaser;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 根据浏览pv和订购pv生成SessionInfo。
 *
 * Created by LiMingji on 2015/5/21.
 */
public class SessionInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    private static Logger log = Logger.getLogger(SessionInfo.class);

    //SessionId
    private String sessionId = null;
    //包含此Session的用户id
    private String msisdnId = null;
    //真实信息费
    private double realInfoFee = 0.0;
    //渠道id 营销ID
    private String channelId = null;
    //产品ID
    private String productId = null;
    private long lastUpdateTime;

    //通过lastUpdateTime算出来的今天是星期几 和昨天是星期几。
    private int day;
    private int yesterday;
    //图书ID
    private String bookId = null;
    //手机号码对应的省ID
    private String provinceId = "";
    //订单类型
    private int orderType = 0;

    //图书阅读浏览pv， 多缓存20 分钟的数据。防止订单时序错乱造成的错误
    private CachedList<String> bookReadPv = new CachedList<String>(Constant.SIXTYFIVE_MINUTES + 2 * 10 * 60);
    //图书购买pv,Key为图书ID Value 为 (key为 OrderType 和 value 订购时间点)组成的Map
    private BookOrderList bookOrderPv = new BookOrderList();
    //各个渠道下的日购买费用 Key为日期。Value为用户日期下的 渠道和渠道下的购买费用。
    private ConcurrentHashMap<Integer, ConcurrentHashMap<String, Double>> channelOrderPv = new ConcurrentHashMap<Integer, ConcurrentHashMap<String, Double>>();
    //用户营销Id对应的扣费二级渠道。 Key是日期，Value为用户日期下的所有二级渠道个数。
    private ConcurrentHashMap<Integer, HashSet<String>> orderChannelCodeByDay = new ConcurrentHashMap<Integer, HashSet<String>>();

    @Override
    public String toString() {
        String context = "msisdnId: " + msisdnId + " realInfoFee : " + realInfoFee + " channelId " + channelId +
                " lastUpdateTime : " + new Date(lastUpdateTime) + " orderType: " + orderType + " \n ";
        context += " 图书浏览pv：" + bookReadPv.toString() + " \n";
        context += " 图书购买pv: " + bookOrderPv.toString() + " \n";
        return context;
    }

    //对应浏览pv 和 订购pv 构建SessionInfo
    public SessionInfo(String sessionId, String msisdnId, String bookReadId,
                       String bookOrderId, String bookChapterOrderId, Long currentTime,
                       int orderType, double realInfoFee, String channelId, String productId, String provinceId) {

        this.sessionId = sessionId;
        this.msisdnId = msisdnId;
        this.orderType = orderType;
        this.realInfoFee = realInfoFee;
        this.channelId = channelId;
        this.bookId = bookOrderId;

        if (currentTime != null) {
            this.lastUpdateTime = currentTime;
        } else {
            this.lastUpdateTime = System.currentTimeMillis();
        }
        if (sessionId == null || msisdnId == null) {
            log.error("sessionId || msisdn should not be null");
            throw new IllegalArgumentException("sessionId || msisdn should not be null");
        }

        if (productId != null) {
            this.productId = productId;
        }
        if (bookReadId != null) {
            bookReadPv.put(bookReadId, lastUpdateTime);
        }
        if (bookOrderId != null && orderType != -1) {
            //为了方便规则7的判断。在此将orderType=21定义为批量订购
            if ((orderType == 2 && bookChapterOrderId == null) ||
                    (orderType == 2 && bookChapterOrderId.trim().equals(""))) {
                this.orderType = 21;
            }
            bookOrderPv.put(bookOrderId, this.orderType, lastUpdateTime);
        }

        this.provinceId = provinceId;

        day = TimeParaser.getDateFromLong(lastUpdateTime);
        yesterday = day == 0 ? 6 : day - 1;

        //统计orderType == 1情况下的用户日渠道信息费。
        if (this.orderType == 1) {
            //清除前一天的数据。
            if (channelOrderPv.containsKey(yesterday)) {
                channelOrderPv.remove(yesterday);
            }
            //如果包含今天的日期。就修改日期内渠道下的异常费用。
            if (channelOrderPv.containsKey(day)) {
                ConcurrentHashMap<String, Double> map = channelOrderPv.get(day);
                if (map.containsKey(channelId)) {
                    double fee = map.get(channelId) + realInfoFee;
                    map.put(channelId, fee);
                } else {
                    map.put(channelId, realInfoFee);
                }
            } else {
                //不包含今天的日期。就将今天的日期添加到map中。
                ConcurrentHashMap<String, Double> map = new ConcurrentHashMap<String, Double>();
                map.put(this.channelId, realInfoFee);
                channelOrderPv.put(day, map);
            }
        }

        //如果不是订购话单就不需要判断扣费的二级渠道了。
        if (this.orderType == -1) {
            return;
        }
        //将用户channelCode对应的二级渠道进行保存
        if (DBStatisticBoltHelper.parameterId2SecChannelId == null || DBStatisticBoltHelper.parameterId2SecChannelId.isEmpty()) {
            try {
                DBStatisticBoltHelper.getData();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        //在二维渠道表中找到二级渠道的话就增加到map中。找不到的话就抛弃。
        if (DBStatisticBoltHelper.parameterId2SecChannelId.containsKey(channelId.toUpperCase())) {
            String secondChannelId = DBStatisticBoltHelper.parameterId2SecChannelId.get(channelId.toUpperCase());
            //清除昨天的数据
            if (this.orderChannelCodeByDay.containsKey(yesterday)) {
                this.orderChannelCodeByDay.remove(yesterday);
            }
            //若今天有数据。则将channel直接add进Set。若今天无数据。创建一个Set存放今天所有的二级渠道。
            if (this.orderChannelCodeByDay.containsKey(day) && this.orderChannelCodeByDay.get(day) != null) {
                this.orderChannelCodeByDay.get(day).add(secondChannelId);
            } else {
                HashSet<String> todayChannelCodes = new HashSet<String>();
                todayChannelCodes.add(secondChannelId);
                this.orderChannelCodeByDay.put(day, todayChannelCodes);
            }
        }
    }

    //对已存在的SessionInfo进行更新。
    public void updateSessionInfo(String sessionId, String bookReadId, String bookOrderId, String bookChapterOrderId,
                                  Long currentTime, int orderType, Double realInfoFee,
                                  String channelId, String productId, String provinceId) {

        this.sessionId = sessionId;
        this.bookId = bookOrderId;
        this.provinceId = provinceId;
        this.orderType = orderType;
        this.realInfoFee = realInfoFee;
        this.channelId = channelId;

        if (currentTime != null) {
            lastUpdateTime = currentTime;
        } else {
            lastUpdateTime = System.currentTimeMillis();
        }
        if (bookReadId != null) {
            bookReadPv.put(bookReadId, lastUpdateTime);
        }
        if (bookOrderId != null && orderType != -1) {
            //为了方便规则7的判断。在此将orderType=21定义为批量订购
            if ((orderType == 2 && bookChapterOrderId == null) ||
                    (orderType == 2 && bookChapterOrderId.trim().equals(""))) {
                this.orderType = 21;
            }
            bookOrderPv.put(bookOrderId, this.orderType, lastUpdateTime);
        }

        if (productId != null) {
            this.productId = productId;
        }

        day = TimeParaser.getDateFromLong(lastUpdateTime);
        yesterday = day == 0 ? 6 : day - 1;

        //统计orderType == 1情况下的用户日渠道信息费。
        if (this.orderType == 1) {
            //清除前一天的数据。
            if (channelOrderPv.containsKey(yesterday)) {
                channelOrderPv.remove(yesterday);
            }
            //如果包含今天的日期。就修改日期内渠道下的异常费用。
            if (channelOrderPv.containsKey(day)) {
                ConcurrentHashMap<String, Double> map = channelOrderPv.get(day);
                if (map.containsKey(channelId)) {
                    double fee = map.get(channelId) + realInfoFee;
                    map.put(channelId, fee);
                } else {
                    map.put(channelId, realInfoFee);
                }
            } else {
                //不包含今天的日期。就将今天的日期添加到map中。
                ConcurrentHashMap<String, Double> map = new ConcurrentHashMap<String, Double>();
                map.put(this.channelId, realInfoFee);
                channelOrderPv.put(day, map);
            }
        }

        //如果不是订购话单就不需要判断扣费的二级渠道了。
        if (this.orderType == -1) {
            return;
        }
        //将用户channelCode对应的二级渠道进行保存
        if (DBStatisticBoltHelper.parameterId2SecChannelId == null || DBStatisticBoltHelper.parameterId2SecChannelId.isEmpty()) {
            try {
                DBStatisticBoltHelper.getData();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        //在二维渠道表中找到二级渠道的话就增加到map中。找不到的话就抛弃。
        if (DBStatisticBoltHelper.parameterId2SecChannelId.containsKey(channelId.toUpperCase())) {
            String secondChannelId = DBStatisticBoltHelper.parameterId2SecChannelId.get(channelId.toUpperCase());
            //清除昨天的数据
            if (this.orderChannelCodeByDay.containsKey(yesterday)) {
                this.orderChannelCodeByDay.remove(yesterday);
            }
            //若今天有数据。则将channel直接add进Set。若今天无数据。创建一个Set存放今天所有的二级渠道。
            if (this.orderChannelCodeByDay.containsKey(day) && this.orderChannelCodeByDay.get(day) != null) {
                this.orderChannelCodeByDay.get(day).add(secondChannelId);
            } else {
                HashSet<String> todayChannelCodes = new HashSet<String>();
                todayChannelCodes.add(secondChannelId);
                this.orderChannelCodeByDay.put(day, todayChannelCodes);
            }
        }
    }

    public boolean clear() {
        //size 自带清理功能。
        return bookReadPv.size(lastUpdateTime, -1) == 0 &&
                bookOrderPv.sizeOfOrderBooks(lastUpdateTime) == 0 &&
                !this.orderChannelCodeByDay.containsKey(day) &&
                !this.channelOrderPv.containsKey(day);
    }

    /**
     * 检测规则 1、2、3
     * 规则1：用户有sessionid且5分钟内，对图书有订购行为，且前1小时后5分钟内对该产品的点击pv=0 ordertype not in ( 4 5 9 99)
     * 规则2：用户有sessionid且5分钟内，对图书有订购行为，且前1小时后5分钟内对该产品的点击pv=1 ordertype not in ( 4 5 9 99)
     * 规则3：用户有sessionid且5分钟内，对图书有订购行为，且前1小时后5分钟内对该产品的点击 1<pv<=5 ordertype not in ( 4 5 9 99)
     *
     * @param bookId
     * @param callback
     */
    public void checkRule123(final String bookId, final RulesCallback callback) {
        if (orderType == 4 || orderType == 5 || orderType == 9 || orderType == 99) {
            return;
        }
        //根据特定图书浏览次数来判断违反的是哪条规则 获取的是当前时间延后5分钟的数据。所以要+5 * 60 * 1000l
        Rules rule = null;
        int thisBookReadPv = bookReadPv.sizeById(bookId, lastUpdateTime + Constant.FIVE_MINUTES * 1000L, Constant.SIXTYFIVE_MINUTES);
        if (thisBookReadPv == Constant.READPV_ZERO_TIMES) {
            rule = Rules.ONE;
        } else if (thisBookReadPv == Constant.READPV_ONE_TIMES) {
            rule = Rules.TWO;
        } else if (thisBookReadPv <= Constant.READPV_THREASHOLD && thisBookReadPv > Constant.READPV_ONE_TIMES) {
            rule = Rules.THREE;
        }
        if (rule != null) {
            callback.hanleData(msisdnId, sessionId, lastUpdateTime, realInfoFee,
                    channelId, productId, rule.name(), provinceId, orderType, bookId);
        }
    }

    /**
     * 检测规则 4
     * 规则5：一个用户日扣费二级渠道>=3个，该用户异常渠道当天所有信息费为异常
     *
     * @param callback
     */
    public void checkRule4(final RulesCallback callback) {
        if (orderChannelCodeByDay.containsKey(day) ) {
            if (orderChannelCodeByDay.get(day).size() >= 3) {
                callback.hanleData(msisdnId, sessionId, lastUpdateTime, realInfoFee, channelId,
                        productId, Rules.FOUR.name(), provinceId, orderType, bookId);
            }
        }
    }

    /**
     * 检测规则 5
     * 规则5：一个用户日渠道ID中的按本订购费>10元，该用户异常渠道当天所有信息费为异常
     * orderType=1 系统中的单位是分。所以下面的10元要换成1000分。
     *
     * @param callback
     */
    public void checkRule5(String channelId, final RulesCallback callback) {
        if (orderType != 1) {
            return;
        }
        if (channelOrderPv.containsKey(day)) {
            ConcurrentHashMap<String, Double> userChannelFee = channelOrderPv.get(day);
            if (userChannelFee.containsKey(channelId) && userChannelFee.get(channelId) > 1000) {
                callback.hanleData(msisdnId, sessionId, lastUpdateTime, realInfoFee,
                        channelId, productId, Rules.FIVE.name(), provinceId, orderType, bookId);
            }
        }
    }

    /**
     * 检测规则 6
     * 规则6：用户有sessionid且3分钟内，包月订购>=2次,回溯一个小时内 异常渠道下的相关费用
     * orderType = 4
     *
     * @param callback
     */
    public void checkRule6(final RulesCallback callback) {
        if (orderType != 4) {
            return;
        }
        int orderTimes = 0;
        for (String bookId : bookOrderPv.keySet()) {
            orderTimes += bookOrderPv.sizeOfBookOrderTimesWithOrderType(bookId, 4, lastUpdateTime, Constant.THREE_MINUTES);
        }
        if (orderTimes >= 2) {
            callback.hanleData(msisdnId, sessionId, lastUpdateTime,
                    realInfoFee, channelId, productId, Rules.SIX.name(), provinceId, orderType, bookId);
        }
    }

    /**
     * 检测规则 7
     * 新版7：用户5分钟内，完本图书订购本数+批量订购本数>=2, 且对订购图书的pv<=5*本数
     * orderType = 1 || ( orderType = 2 且 chapterId == null ) 将此情况的orderType 定为 21
     *
     * @param callback
     */
    public void checkRule7(final RulesCallback callback) {
        if (orderType == 1 || orderType == 21) {
            int bookOrderNums = 0;
            int bookReadPvs = 0;
            for (String bookId : bookOrderPv.keySet()) {
                bookOrderNums += bookOrderPv.sizeOfBookOrderTimesWithOrderType(bookId, 1, lastUpdateTime, Constant.FIVE_MINUTES)
                        + bookOrderPv.sizeOfBookOrderTimesWithOrderType(bookId, 21, lastUpdateTime, Constant.FIVE_MINUTES);
                bookReadPvs += bookReadPv.sizeWithTimeThreshold(bookId, lastUpdateTime, Constant.FIVE_MINUTES);
            }
            if (bookOrderNums >= 2 && bookReadPvs <= 5 * bookOrderNums) {
                callback.hanleData(msisdnId, sessionId, lastUpdateTime, realInfoFee,
                        channelId, productId, Rules.SEVEN.name(), provinceId, orderType, bookId);
            }
        }
    }

    /**
     * 检测规则 8
     * 规则8：用户5分钟内，连载图书订购章数>=10，且对订购图书的pv<=2*章数
     * 不满10个。后续判断不触发。
     * orderType=2
     *
     * @param callback
     */
    public void checkRule8(String bookId, final RulesCallback callback) {
        if (orderType != 2) {
            return;
        }
        int orderPvs = bookOrderPv.sizeOfBookOrderTimesWithOrderType(bookId, 2, lastUpdateTime, Constant.FIVE_MINUTES);
        int readPvs = bookReadPv.sizeWithTimeThreshold(bookId, lastUpdateTime, Constant.FIVE_MINUTES);
        if (orderPvs >= 10 && readPvs <= 2 * orderPvs) {
            callback.hanleData(msisdnId, sessionId, lastUpdateTime, realInfoFee,
                    channelId, productId, Rules.EIGHT.name(), provinceId, orderType, bookId);
        }
    }

    /**
     * 规则12：用户有sessionid且属于非BOSS包月，前1小时后5分钟内图书的总pv=0
     * ordertype = 4 accesstype <> 6
     *
     * @param platform  platform = accesstype
     * @param callback
     */
    public void checkRule12(String platform, final RulesCallback callback) {
        if (orderType == 4 && Integer.parseInt(platform) != 6) {
            //将当前时间推后5分钟。然后取前65分钟的pv数
            if (bookReadPv.size(lastUpdateTime + Constant.FIVE_MINUTES * 1000l, Constant.SIXTYFIVE_MINUTES) == 0) {
                callback.hanleData(msisdnId, sessionId, lastUpdateTime, realInfoFee,
                        channelId, productId, Rules.TWELVE.name(), provinceId, orderType, bookId);
            }
        }
    }
}