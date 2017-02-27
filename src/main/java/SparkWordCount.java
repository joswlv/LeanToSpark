import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;


/**
 * Created by Jo_seungwan on 2017. 2. 27..
 */

public class SparkWordCount {

    public static void main(String[] args) {
        String logFile = "/Users/Jo_seungwan/spark/README.md"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        JavaPairRDD<String, Integer> counts = logData
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a,b) -> a + b);

        counts.saveAsTextFile("/Users/Jo_seungwan/spark/result");

        sc.stop();
    }
}
