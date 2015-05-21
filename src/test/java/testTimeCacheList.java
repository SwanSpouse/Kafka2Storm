import com.order.util.TimeCacheList;

/**
 * Created by LiMingji on 2015/5/21.
 */
public class testTimeCacheList {
    public static TimeCacheList<String> timeCacheList = null;
    public static Thread thread = null;
    public static void main(String[] args) throws InterruptedException {


        timeCacheList = new TimeCacheList<String>(10,new TimeCacheList.ExpiredCallback<String>() {
            @Override
            public void expire(String key) {
                System.out.println("过期的数据:" + key );
            }
        });
        thread = new Thread(new Runnable() {
            @Override
            public void run() {
                int count = 1;
                while (true) {
                    try {
                        timeCacheList.put("This is ===> " + count );
                        System.out.println("插入的数据" + count + "==>" + "test currentSize " + timeCacheList.size());
                        if (count %4 == 0) {
                            System.out.println("刚刚插入的数据" + timeCacheList.getLast() + "==>" + "test currentSize " + timeCacheList.size());
                        }
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
