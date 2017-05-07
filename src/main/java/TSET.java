import org.junit.Test;

import java.net.URLDecoder;

public class TSET {

    @Test
    public void UrlDecoderTest() {
        try {
            String url = "http%3A%2F%2Fv.media.daum.net%2Fv%2F20170507090013189%3Ff%3Dm";
            String result = URLDecoder.decode(url,"UTF-8");
            result = URLDecoder.decode(result,"UTF-8");
            System.out.println(result);
        } catch (Exception e){

        }
    }
}
