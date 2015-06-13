import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Iterator;

/**
 * Created by LiMingji on 2015/6/12.
 */
public class OrderData {

    ////将订单数据从json中解析出来
    public static String splitJson(String msg) {
        try {
            JSONObject msgJson = new JSONObject(msg);
            Iterator<String> msgIt = msgJson.keys();
            while (msgIt.hasNext()) {
                String msgKey = msgIt.next();
                if (msgKey.equals("body")) {
                    JSONObject cdrJson = new JSONObject(msgJson.getString(msgKey));
                    Iterator<String> cdrIt = cdrJson.keys();
                    while (cdrIt.hasNext()) {
                        String cdrkey = cdrIt.next();
                        if (cdrkey.equals("cdr")) {
                            return cdrJson.getString(cdrkey);
                        }
                    }
                }
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static void main(String[] args) throws JSONException {
        String body = "{\"body\":{\"cdr\":\"80086409468|20150611172631|mozilla/4.0 (compatible; msie 8.0; windows nt 6.1; win64; x64; trident/4.0; .net clr 2.0.50727; slcc2; .net clr 3.5.30729; .net clr 3.0.30729; media center pc 6.0)|2|2|1234793|zhongqirm|402494931|406348551|119|||||10|12|0|0|80086409468|2|2|591888|000|000|127.0.0.1|4|3||15061108372285c01ac2129923b00000|1||1|80086409468|||||||||10.211.57.100||||||||\"},\"seqid\":\"1\",\"tags\":[\"ireadcharge11\"],\"topic\":\"report.cdr\",\"type\":\"report.cdr\"}";

        String line = splitJson(body);
        System.out.println(line);

        String[] words = line.split("\\|", -1);
        if (words.length >= 49) {
            String msisdn = words[0]; // msisdnID Varchar2(20)
            String recordTime = words[1]; // Recordtime Varchar2(14)
            String terminal = words[2];// UA Varchar2(255)
            String platform = words[3];// 门户类型numbser(2)
            String orderType = words[4]; // 订购类型  number(2)
            String productID = words[5];// 产品ID Varchar2(32)
            String bookID = words[7]; // 图书ID Number(19)
            String chapterID = words[8]; // 章节ID Varchar2(32)
            String channelCode = words[9];// 渠道ID Varchar2(8)
            String cost = words[14]; // 费用 Number(12,4)
            String provinceId = words[22]; // Varchar2(16)
            String wapIp = words[24]; // IP地址 Varchar2(40)
            String sessionId = words[39];// sessionId Varchar2(255)
            String promotionid = words[40]; // 促销互动ID (废弃) 2015-06-05

            System.out.println("msisdn: " + msisdn + " recordTime " + recordTime + " UA" + terminal
                    + " platform " + platform + " orderType " + orderType + " productId " + productID +
                    " bookId " + bookID + " chapterId " + chapterID + " channelCode " + channelCode + " cost " + cost
                    + " provinceId " + provinceId + " wapId " + wapIp + " sessionId " + sessionId);
        }
    }
}
