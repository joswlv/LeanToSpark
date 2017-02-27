import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;


/**
 * Created by Jo_seungwan on 2017. 2. 27..
 */

public class SparkWordCount {

    public static void main(String[] args) {
        String logFile = "/Users/Jo_seungwan/spark/README.md"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        //countByKey()를 사용하여 빠르게 단어수 얻기
        Map<String, Long> fastCount = logData
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .countByValue();

        for (String key : fastCount.keySet()) {
            System.out.println(key + ": "+fastCount.get(key));
        }

        //pair를 사용하기
        //new Tuple2<>(word, 1)은 듀플 자료형으로 (단어, 1)이런식으로 저장해라는 뜻이다.
        //java에는 Tuple자료형이 없기 때문에 spark에서 제공하는 Tuple2()함수를 사용한다.
        JavaPairRDD<String, Integer> counts = logData
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a,b) -> a + b);

        //lambda식 풀어 쓰기
        JavaRDD<String> word = logData.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> result = word.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2(s,1);
                    }
                }
        ).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer a, Integer b) throws Exception {
                        return a+b;
                    }
                }
        );

        counts.saveAsTextFile("/Users/Jo_seungwan/spark/result");

        sc.stop();
    }
}
