package imitatedata;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by LiMingji on 2015/6/12.
 */
public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {

        MsgProducer msgProducer = new MsgProducer();

        File file = new File("data.txt");
        FileReader fileReader = new FileReader(file);
        BufferedReader br = new BufferedReader(fileReader);
        String line = "";
        int num = 1;
        while ((line = br.readLine()) != null) {
            String[] params = line.split("==>");
            if (params.length != 2) {
                System.out.println("error line: " + line);
                continue;
            }
            String topic = params[0];
            String msg = params[1];

            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");

            if (topic.equals("first")) {
                msg = msg.replaceAll("20150611172631", df.format(new Date()));
                msgProducer.sendMsg(KafkaProperties.viewTopic, msg);
            }else if (topic.equals("second")) {
                msg = msg.replaceAll("20150611172631", df.format(new Date()));
                msgProducer.sendMsg(KafkaProperties.orderTopic, msg);
            }else if (topic.equals("sleep")) {
                long ms = Long.parseLong(msg) * 1000;
                Thread.sleep(ms);
            }
            System.out.println(num + "=>" + msg);
            num ++ ;
        }
    }
}
