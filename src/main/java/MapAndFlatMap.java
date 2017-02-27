import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by Jo_seungwan on 2017. 2. 27..
 */
public class MapAndFlatMap {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("MapAndFlatMap");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> number = sc.parallelize(Arrays.asList(1,2,3,4,5));
        JavaRDD<Integer> numberResult = number.map(n -> n*n);

        System.out.println(StringUtils.join(numberResult.collect(),","));

        JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world", "hi world", "good world"));
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        for (String s : words.collect()) {
            System.out.println(s);
        }

    }
}
