package driver;

import common.DefaultSetting;
import function.DemoFunction;
import model.DemoInfoKey;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class UniqCountBySubtract {

    private String appName;
    private String input1;
    private String input2;
    private String outputDir;
    private String configFile;

    private void init(String[] args) {

        //configuration setting
        appName = args[0];
        configFile = args[1];
        input1 = args [2];
        input2 = args [3];
        outputDir = args[4];
    }

    private void run(String[] args) {
        init(args);

        SparkConf jobConf = DefaultSetting.getDefaultSparkConfig(appName,configFile);
        JavaSparkContext sc = new JavaSparkContext(jobConf);

        JavaRDD<DemoInfoKey> nowRDD = sc.textFile(input1).map(DemoFunction.getRefineLogParserFunction());
        JavaRDD<DemoInfoKey> beforeRDD = sc.textFile(input2).map(DemoFunction.getRefineLogParserFunction());

        JavaRDD<DemoInfoKey> resultRDD = nowRDD.subtract(beforeRDD);
        System.out.println(resultRDD.take(10));
        resultRDD.map(DemoFunction.getRefinePrintFunction()).saveAsTextFile(outputDir);

        sc.close();
    }

    public static void main(String[] args) {new UniqCountBySubtract().run(args);}
}
