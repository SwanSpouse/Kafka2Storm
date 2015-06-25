import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by LiMingji on 2015/6/25.
 */
public class GetWord {
    public static void main(String[] args) throws IOException {
        File file = new File("data.txt");
        FileReader fileReader = new FileReader(file);
        BufferedReader br = new BufferedReader(fileReader);
        String line = "";
        while ((line = br.readLine()) != null) {
            String line2 = line.split("==>")[1];
            String[] words = line2.split("\\|");
            int count = 0;
            for (String word : words) {
                System.out.println(count + " " + word);
                count++;
            }
        }
    }
}
