package testspout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;

import com.order.util.FName;
import com.order.util.StreamId;
import com.order.util.TimeParaser;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class DataWarehouseSpout extends BaseRichSpout{
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	static Logger log = Logger.getLogger(DataWarehouseSpout.class);
	private boolean completed = false;
	private FileReader fileReader;

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		try {
			this.fileReader = new FileReader(new File("/home/hadoop/ghb/data.txt"));
		} catch (FileNotFoundException e) {
			throw new RuntimeException("error");
		}
		this.collector = collector;
	}

	public void nextTuple() {
		if(completed){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			return;
		}
		BufferedReader br =new BufferedReader(fileReader);
		try{
	        String line = "";
	        int num = 1;
	        int oknum = 1;
	        int failnum = 1;
	        while ((line = br.readLine()) != null) {
	            String[] params = line.split("==>");
	            if (params.length != 2) {
	                System.out.println("error line: " + line);
	                continue;
	            }
	            String topic = params[0];
	            String msg = params[1];
	            if (topic.equals("sleep")) {
	            	long ms = Long.parseLong(msg) * 1000;
	            	Thread.sleep(ms);
	            }
	            else {
	            	// 替换时间
		            //SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
	                //msg = msg.replaceAll("20150611172631", df.format(new Date()));
	                
	                String[] msgs = msg.split("\\|");
	                // 拆分
/*		            String msisdn = msgs[0] + String.valueOf(num);
		            String sessionId = msgs[1] + String.valueOf(num);
		            Long recordTime = TimeParaser.splitTime(msgs[2]);
		            double realInfoFee = Double.parseDouble(msgs[3]);
		            String channelCode = msgs[4];
		            String productId = msgs[5] + String.valueOf(num);
		            int provinceId = Integer.parseInt(msgs[6]);
		            int orderType = Integer.parseInt(msgs[7]);
		            String bookId = msgs[8] + String.valueOf(num);
*/
		            String msisdn = msgs[0];
		            String sessionId = msgs[1];
		            Long recordTime = TimeParaser.splitTime(msgs[2]);
		            double realInfoFee = Double.parseDouble(msgs[3]);
		            String channelCode = msgs[4];
		            String productId = msgs[5];
		            String provinceId = msgs[6];
		            int orderType = Integer.parseInt(msgs[7]);
		            String bookId = msgs[8];

	                if (topic.equals("okorder"))
	                {
	                	msisdn += String.valueOf(oknum);
	                	bookId += String.valueOf(oknum);
	                	log.info("======size of ok order is " + String.valueOf(oknum) + "========="); // test
	                	oknum++;
			            collector.emit(StreamId.DATASTREAM.name(), new Values(msisdn, sessionId, recordTime,
		                    realInfoFee, channelCode, productId, provinceId, orderType, bookId));
	                }
	                else if (topic.equals("failorder")) {
	                	msisdn += String.valueOf(failnum);
	                	bookId += String.valueOf(failnum);
	                	log.info("======size of fail order is " + String.valueOf(failnum) + "========="); // test
	                	failnum++;
	                	String rule = msgs[9];
	                	collector.emit(StreamId.ABNORMALDATASTREAM.name(),
	                			new Values(msisdn, sessionId, recordTime, realInfoFee, channelCode, 
	                					productId, provinceId, orderType, bookId, rule));
	                }
	            	
	            }
	            System.out.println(num + "=>" + msg);
	            num ++ ;
	        }
        	log.info("======size of fail order is " + String.valueOf(num) + "========="); // test
		} catch (Exception e) {
			throw new RuntimeException("Error reading tuple",e);
		} finally {
			completed = true;
		}
	}


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declare(new Fields(FName.LINE.name()));
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

	public void close() {
	}

	public void ack(Object msgId) {
		super.ack(msgId);
	}
}
