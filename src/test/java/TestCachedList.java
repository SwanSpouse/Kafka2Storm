import com.order.databean.TimeCacheStructures.CachedList;

/**
 * Created by LiMingji on 2015/6/20.
 */
public class TestCachedList {

    public static void main(String[] args) {
        CachedList<Integer> list = new CachedList<Integer>(2);

        list.put(2, 2000L);
        list.put(2, 2001L);

        list.put(3, 1000L);
        list.put(3, 3001L);
        list.put(3, 3002L);

        System.out.println(list.size(4000L, -1));
        System.out.println(list);
    }
}
