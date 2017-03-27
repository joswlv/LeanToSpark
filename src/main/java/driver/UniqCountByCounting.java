package driver;

import common.DefaultSetting;
import function.DemoFunction;
import model.DemoInfoKey;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by nhnent on 2017. 3. 27..
 */
public class UniqCountByCounting {

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

        JavaRDD<String> nowRDD = sc.textFile(input1);
        JavaRDD<String> beforeRDD = sc.textFile(input2);

        JavaPairRDD<DemoInfoKey,Integer> resultRDD;
        resultRDD = nowRDD.union(beforeRDD).mapToPair(DemoFunction.getRefineLogParserPairFunction())
                .reduceByKey(DemoFunction.getDemoCount())
                .filter(DemoFunction.getDemoFilter());

        resultRDD.map(DemoFunction.getRefinePrintFunction()).saveAsTextFile(outputDir);

        sc.close();
    }

    public static void main(String[] args) {new UniqCountByCounting().run(args);}
}
