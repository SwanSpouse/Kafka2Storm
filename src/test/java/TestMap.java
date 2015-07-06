import java.util.HashMap;

/**
 * Created by LiMingji on 15/7/6.
 */
public class TestMap {
    private static class A {
        public HashMap<Integer, Integer> map = null;

    }

    private static class B {
        public HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();

        public B() {
            map.put(1, 1);
            map.put(2, 1);
        }
    }

    public static void main(String[] args) {
        A a = new A();
        B b = new B();

        a.map = b.map;

        System.out.println(a.map);
        a.map.put(3, 1);

        System.out.println(a.map);
        System.out.println(b.map);
        b.map = null;
        System.out.println(a.map);
    }
}
