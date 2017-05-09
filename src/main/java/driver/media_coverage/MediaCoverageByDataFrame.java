package driver.media_coverage;

import common.LocalDefaultSetting;
import function.media_coverage.MediaFunction;
import model.media_coverage.MediaLogModel;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * Created by Jo_seungwan on 2017. 5. 9..
 */
public class MediaCoverageByDataFrame {
    private String appName;
    private String input1;
    private String input2;
    private String outputDir;

    private void init(String[] args) {

        //configuration setting
        appName = args[0];
        input1 = args [1];
        input2 = args [2];
        outputDir = args[3];
    }

    private void run(String arg[]){
        init(arg);

        SparkConf jobConf = LocalDefaultSetting.getDefaultSparkConfig(appName);
        JavaSparkContext sc = new JavaSparkContext(jobConf);

        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<MediaLogModel> dmpLogBid = sc.textFile(input1).map(MediaFunction.getBidFromDmpLog());
        JavaRDD<MediaLogModel> dspLogBid = sc.textFile(input2).map(MediaFunction.getBidFromDspLog());

        Dataset<Row> dmpBidDataFrame = sqlContext.createDataFrame(dmpLogBid, MediaLogModel.class);
        Dataset<Row> dspBidDataFrame = sqlContext.createDataFrame(dspLogBid, MediaLogModel.class).distinct();

        long dmpBidCount = dmpBidDataFrame.count();

        dmpBidDataFrame.createOrReplaceTempView("dmp");
        dspBidDataFrame.createOrReplaceTempView("dsp");

        Dataset<Row> result = sqlContext.sql("select dmp.bid from dmp join dsp where dmp.bid = dsp.bid");
        long resultcount = result.count();

        System.out.println();
        System.out.println("dmpCount => "+dmpBidCount);
        System.out.println("reslutCount=> "+resultcount);
        System.out.println();
    }

    public static void main(String[] args) {
        new MediaCoverageByDataFrame().run(args);
    }
}
