
import java.io.*;
import java.util.ArrayList;

import com.order.db.DBHelper.DBDataWarehouseCacheHelper;
import com.order.util.OrderRecord;
import com.order.util.TimeParaser;

/**
 * Created by Guohongbo on 2015/6/16.
 */
public class DBInsertTest {
    public static void main(String[] args) throws IOException, InterruptedException {
    	
    	DBDataWarehouseCacheHelper cache = new DBDataWarehouseCacheHelper();
    	
    	cache.insertData("msisdn1", "session", "channelcode2", TimeParaser.splitTime("20150616182800"), "bookid1", "productid", 500, "1", 2);
    	cache.insertData("msisdn1", "session", "channelcode2", TimeParaser.splitTime("20150616185800"), "bookid2", "productid", 500, "1", 2);
    	cache.insertData("msisdn1", "session", "channelcode2", TimeParaser.splitTime("20150616185900"), "bookid3", "productid", 500, "1", 2);
    	Thread.sleep(2*1000);
    	cache.updateData("msisdn1", "session", "channelcode2", TimeParaser.splitTime("20150616185800"), "bookid2", "productid", 500, "1", 2, "TWO");
    	//cache.updateData("msisdn2", "session", "channelcode", "20150616184000", "bookid", "productid", 500, "TWELVE");
    	Thread.sleep(2*1000);
    	
    	
    	ArrayList<OrderRecord> list = cache.traceBackOrders("msisdn1", "channelcode2", TimeParaser.splitTime("20150616183900"), 2);
    	System.out.print("list ============\n");
    	for (int i=0; i<list.size(); i++) {
    		System.out.print(list.get(i).toString() + "\n");
    	}
    	Thread.sleep(2000*1000);

    	return;  	
    }
}
