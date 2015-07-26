import java.util.TreeMap;

/**
 * Created by LiMingji on 15/7/25.
 */
public class TestTreeMap {

    public static void main(String[] args) {
        TreeMap<Integer, Integer> map = new TreeMap<Integer, Integer>();
        map.put(3, 3);
        map.put(2, 2);
        map.put(1, 1);
        map.put(4, 4);
        map.put(6, 6);
        map.put(5, 5);
        System.out.println(map.keySet());
    }
}
