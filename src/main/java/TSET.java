import java.net.URLDecoder;

public class TSET {
    public static void main(String[] args) {
        try {
            String url = "%257B%2522";
            String result = URLDecoder.decode(url,"UTF-8");
            result = URLDecoder.decode(result,"UTF-8");
            System.out.println(result);
        } catch (Exception e){

        }
    }
}
