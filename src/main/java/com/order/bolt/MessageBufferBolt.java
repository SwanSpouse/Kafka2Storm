package com.order.bolt;

import java.util.LinkedList;
import java.util.Map;

import com.order.util.OrderItem;
import com.order.util.TimeParaser;

import org.apache.log4j.Logger;

import com.order.util.FName;
import com.order.util.StreamId;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MessageBufferBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 1L;
    static Logger log = Logger.getLogger(MessageBufferBolt.class);
    private static final long FIVEMINUTES = 5 * 60 * 1000L;

    private BasicOutputCollector collector = null;

    //缓存的订购消息
    private LinkedList<OrderItem> orderQueue = null;
    //最近一条浏览消息的时间
    private long lastViewTime = 0;
    // 上次定时清理检查时间
    private long preViewTime = 0;

	private transient Thread cleaner = null; // 清理线程
    private transient Object LOCK = null;

    @Override
    public void prepare(Map conf, TopologyContext context) {
        super.prepare(conf, context);
        orderQueue = new LinkedList<OrderItem>();
    }
    
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        this.createCleanThread();

        LOCK = LOCK == null ? new Object() : LOCK;
        synchronized (LOCK) {
            this.collector = collector;
            if (input.getSourceStreamId().equals(StreamId.BROWSEDATA.name())) {
                // 浏览消息
                Long recordTime = TimeParaser.splitTime(input.getStringByField(FName.RECORDTIME.name()));
                String sessionId = input.getStringByField(FName.SESSIONID.name());
                String pageType = input.getStringByField(FName.PAGETYPE.name());
                String msisdn = input.getStringByField(FName.MSISDN.name());
                String channelCode = input.getStringByField(FName.CHANNELCODE.name());
                String bookId = input.getStringByField(FName.BOOKID.name());
                String chapterId = input.getStringByField(FName.CHAPTERID.name());
                collector.emit(StreamId.BROWSEDATA.name(), new Values(
                        recordTime, sessionId, pageType, msisdn,
                        channelCode, bookId, chapterId));
                lastViewTime = recordTime;
            } else if (input.getSourceStreamId().equals(StreamId.ORDERDATA.name())) {
                // 订购消息
                String msisdn = input.getStringByField(FName.MSISDN.name());
                Long recordTime = input.getLongByField(FName.RECORDTIME.name());
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
                String promotionid = input.getStringByField(FName.PROMOTIONID.name());
                OrderItem item = new OrderItem(msisdn, recordTime, userAgent, platform,
                        orderTypeStr, productId, bookId, chapterId,
                        channelCode, realInfoFeeStr, provinceId, wapIp,
                        sessionId, promotionid);
                orderQueue.addLast(item);
            }
            // 不管收到订购消息还是浏览消息，都检查一下是否需要将缓存发送（后续可改为收到订购消息检查）。
            emitCachedOrderData(collector, lastViewTime - FIVEMINUTES);
        }
    }
    
    private void createCleanThread() {	
		if (cleaner == null) {
			cleaner = new Thread(new Runnable() {
				@Override
				public void run() {
					while (true) {
						try {
                            // 每隔一个一段时间清理一次。
                            cleaner.sleep(FIVEMINUTES);
                            if (LOCK == null)
                                LOCK = new Object();
                            synchronized (LOCK) {
                                //如果lastViewTime在这10分钟内无变化，说明已无消息，清理入库
                                if (lastViewTime == preViewTime) {
                                    log.info("Begin Clean Order Message Buffer ...");
                                    emitCachedOrderData(collector, System.currentTimeMillis());
                                } else {  //如果lastViewTime在这5分钟内有变化，则无需做操作
                                    preViewTime = lastViewTime;
                                }
                            }
                        } catch (InterruptedException e) {
							e.printStackTrace();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			});
			cleaner.setDaemon(true);
			cleaner.start();
		}
    }
    
    /**
     *  发送浏览消息5分钟前的订单数据。
     *  orderQueue是个队列。按照到达的时间进行插入。每次插入的时候都会从队首取出最近一条浏览数据5分钟前的数据发射出去。
     */
    private void emitCachedOrderData(BasicOutputCollector collector, long time2emitData) {
        while (!orderQueue.isEmpty()) {
            OrderItem item = orderQueue.getFirst();
            if (item.getRecordTime() < time2emitData) {
                collector.emit(StreamId.ORDERDATA.name(), new Values(
                        item.getMsisdn(), item.getRecordTime(), item.getTerminal(), item.getPlatform(),
                        item.getOrderType(), item.getProductID(), item.getBookID(), item.getChapterID(),
                        item.getChannelCode(), item.getCost(), item.getProvinceId(), item.getWapIp(),
                        item.getSessionId(), item.getPromotionid()
                ));
                //数据发送之后将数据移除。
                orderQueue.removeFirst();
            } else {
                return;
            }
        }
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamId.ORDERDATA.name(),
                new Fields(FName.MSISDN.name(), FName.RECORDTIME.name(),
                        FName.TERMINAL.name(), FName.PLATFORM.name(),
                        FName.ORDERTYPE.name(),
                        FName.PRODUCTID.name(), FName.BOOKID.name(),
                        FName.CHAPTERID.name(), FName.CHANNELCODE.name(),
                        FName.COST.name(), FName.PROVINCEID.name(),
                        FName.WAPIP.name(), FName.SESSIONID.name(),
                        FName.PROMOTIONID.name()));
        declarer.declareStream(StreamId.BROWSEDATA.name(),
                new Fields(FName.RECORDTIME.name(),
                        FName.SESSIONID.name(), FName.PAGETYPE.name(), FName.MSISDN.name(),
                        FName.CHANNELCODE.name(), FName.BOOKID.name(), FName.CHAPTERID.name()));
    }

}
