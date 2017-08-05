import org.apache.commons.collections.iterators.ArrayListIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by Jo_seungwan on 2017. 2. 27..
 */
public class MapAndFlatMap {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("MapAndFlatMap");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> number = sc.parallelize(Arrays.asList(1,2,3,4,5));
        JavaRDD<Integer> numberResult = number.map(new Function<Integer, Integer>() {
	        @Override
	        public Integer call(Integer n) throws Exception {
		        return n*n;
	        }
        });

        System.out.println(StringUtils.join(numberResult.collect(),","));

        JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world", "hi world", "good world"));
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
	        @Override
	        public Iterator<String> call(String s) throws Exception {
		        return new ArrayListIterator(s.split(" "));
	        }
        });

        for (String s : words.collect()) {
            System.out.println(s);
        }

    }
}
