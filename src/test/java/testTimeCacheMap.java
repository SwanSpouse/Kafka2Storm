import com.order.util.TimeCacheStructures.TimeCacheMap;

/**
 * Created by LiMingji on 2015/5/21.
 */
public class testTimeCacheMap {
    public static TimeCacheMap<String, String> timeCacheMap = null;
    public static Thread thread = null;
    public static void main(String[] args) throws InterruptedException {
        timeCacheMap = new TimeCacheMap<String, String>(6,5,new TimeCacheMap.ExpiredCallback<String, String>() {
            @Override
            public void expire(String key, String val) {
                System.out.println("过期的数据" + key + "==>" + val);
            }
        });
        thread = new Thread(new Runnable() {
            @Override
            public void run() {
                int count = 1;
                while (true) {
                    try {
                        timeCacheMap.put("This is the " + count + "th", "test");
                        System.out.println("插入的数据" + count + "==>" + "test currentSize " + timeCacheMap.size());
                        thread.sleep(2000);
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
