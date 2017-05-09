package function.media_coverage;

import model.media_coverage.MediaLogModel;
import org.apache.spark.api.java.function.Function;
import com.google.common.base.Splitter;
/**
 * Created by Jo_seungwan on 2017. 5. 9..
 */
public class MediaFunction {

    /* dmp-log fromat
     * [time]\t[ip]\t[bid]\t....
     */
    public static final Function BidFromDmpLog = new Function<String,MediaLogModel>() {
        @Override
        public MediaLogModel call(String log) throws Exception {
            return new MediaLogModel(log.split("\t")[2]);
        }
    };

    /* dsp-log fromat
     *
     * [ip]\t[time]\t ..&..&..&dmp=bid&..&..~~
     */
    public static final Function BidFromDspLog = new Function<String,MediaLogModel>() {
        @Override
        public MediaLogModel call(String log) throws Exception {

            for (String refineLog : Splitter.on("&").split(log.split("\t")[2])){
                String[] resultLog = refineLog.split("=");
                if (resultLog[0].equals("dmp")) {
                    if ( !resultLog[1].equals("-") ){
                        return new MediaLogModel(resultLog[1]);
                    }
                }
            }
            return new MediaLogModel("-");
        }
    };

    public static Function getBidFromDspLog(){
        return BidFromDspLog;
    }
    public static Function getBidFromDmpLog(){
        return BidFromDmpLog;
    }
}
