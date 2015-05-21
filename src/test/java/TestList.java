import java.util.LinkedList;

/**
 * Created by LiMingji on 2015/5/21.
 */
public class TestList {
    public static void main(String[] args) {
        LinkedList<Integer> list = new LinkedList<Integer>();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        list.add(5);

        System.out.println(list.getLast());
        System.out.println(list.getFirst());
    }
}
