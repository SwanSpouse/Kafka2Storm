import java.util.HashMap;

/**
 * Created by LiMingji on 2015/5/24.
 */
public class TestListContains {
    public static void main(String[] args) {
        HashMap<String, String> map = new HashMap<String, String>();
        System.out.println(map.get("limingji").length());
        map.put("limingji", "String");
        System.out.println(map.get("limingji").length());
    }
}
