import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by LiMingji on 2015/5/22.
 */
public class TestTime {

    public static void main(String[] args) throws InterruptedException {
//        long currentTime = System.currentTimeMillis();
//        Thread.sleep(3 * 1000);
//        long next3Second = System.currentTimeMillis();
//        System.out.println("currentTime " + currentTime/1000 + " next3Second " + next3Second/1000);


        //2015 05 05 16 55 23
        String input = "20150505165523";
        Calendar calendar = Calendar.getInstance();

        int year = Integer.parseInt(input.substring(0, 4));
        int month = Integer.parseInt(input.substring(4, 6));
        int date = Integer.parseInt(input.substring(6, 8));
        int hour = Integer.parseInt(input.substring(8, 10));
        int minute = Integer.parseInt(input.substring(10, 12));
        int seconds = Integer.parseInt(input.substring(12, 14));

        System.out.println(year + " " + month + " " + date + " " + hour + " " + minute + " " + seconds);

        calendar.set(year, month, date, hour, minute, seconds);

        System.out.println(calendar.getTime());
        System.out.println(calendar.getTimeInMillis());

        Date hh = new Date();
        hh.setTime(1433494523503L);
        System.out.println(hh);
    }
}
