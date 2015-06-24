import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by LiMingji on 2015/6/21.
 */
public class TsetMap {

    public static void main(String[] args) {
        ConcurrentHashMap<String, Double> map = new ConcurrentHashMap<String, Double>();
        map.put("20150621|0|119|402494931|3", 60.0);
        if (map.containsKey("20150621|0|119|402494931|3")) {
            System.out.println(true);
        }
        String key = "20150621|0|119|402494931|3|1";
        String[] keys = key.split("\\|");
        System.out.println(Arrays.asList(keys));
        String date = keys[0];
        String provinceId = keys[1];
        String channelCode = keys[2];
        String contentID = keys[3];
        String contentType = keys[4];
        String ruleID = keys[5];

        String totalFeeKey = date + "|" + provinceId + "|" + channelCode + "|"
                + contentID + "|" + contentType;
        System.out.println(totalFeeKey);
        if (map.containsKey(totalFeeKey)) {
            System.out.println(true);
        } else {
            System.out.println(false);
        }
    }
}
