import com.order.databean.TimeCacheStructures.Pair;

import java.util.HashMap;

/**
 * Created by LiMingji on 2015/5/27.
 */
public class TestPair {

    public static void main(String[] args) {
        HashMap<Pair, Integer> map = new HashMap<Pair, Integer>();

        Pair<String, Integer> pair = new Pair<String, Integer>("limingji", 0);
        Pair<String, Integer> pair1 = new Pair<String, Integer>("limingji", 1);
        map.put(pair1, pair1.getValue());
        map.put(pair, pair.getValue());

        System.out.println(map);

        Pair<String, Integer> pair2 = new Pair<String, Integer>("Hehe", 1);
        map.put(pair2, pair2.getValue());

        System.out.println(map.get(new Pair("Hehe", null)));
        System.out.println(map);

    }
}
