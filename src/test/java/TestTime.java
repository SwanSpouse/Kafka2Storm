/**
 * Created by LiMingji on 2015/5/22.
 */
public class TestTime {

    public static void main(String[] args) throws InterruptedException {
        long currentTime = System.currentTimeMillis();
        Thread.sleep(3 * 1000);
        long next3Second = System.currentTimeMillis();

        System.out.println("currentTime " + currentTime/1000 + " next3Second " + next3Second/1000);
    }
}
