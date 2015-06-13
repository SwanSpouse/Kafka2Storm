package imitatedata;

import java.io.*;

/**
 * Created by LiMingji on 2015/6/12.
 */
public class Main {
    public static void main(String[] args) throws IOException {

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

            if (topic.equals("first")) {
                msgProducer.sendMsg(KafkaProperties.viewTopic, msg);
            }else if (topic.equals("second")) {
                msgProducer.sendMsg(KafkaProperties.orderTopic, msg);
            }
            System.out.println(num + " => " + msg);
            num += 1;
        }
    }
}
