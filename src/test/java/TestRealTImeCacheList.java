import com.order.databean.TimeCacheStructures.RealTimeCacheList;

import java.util.LinkedList;

/**
 * Created by LiMingji on 2015/5/21.
 */
public class TestRealTimeCacheList {
    public static RealTimeCacheList<String> timeCacheList = null;
    public static Thread thread = null;
    public static void main(String[] args) throws InterruptedException {


        timeCacheList = new RealTimeCacheList<String>(5 ,new RealTimeCacheList.TimeOutCallback<String>() {
            @Override
            public void expire(String value, LinkedList<Long> pvTimes) {
                System.out.println("过期的数据:" + value );
            }
        });
        thread = new Thread(new Runnable() {
            @Override
            public void run() {
                int count = 1;
                while (true) {
                    try {
                        timeCacheList.put("This is ===> " + count % 4);
                        System.out.println("插入的数据" + count + "==>" + "test currentSize " + timeCacheList.size());
                        thread.sleep(1000);
                        count += 1;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        thread.start();
        System.out.println("线程启动");
        Thread.sleep(1000000000);
    }
}
