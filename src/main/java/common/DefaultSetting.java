package common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

/**
 * spark_test - 2017. 3. 27..
 */
public class DefaultSetting {
    public static SparkConf getDefaultSparkConfig(String appName, String configFilePath){
        SparkConf sparkConf = DefaultSetting.getDefaultSparkConfig(appName);
        Properties prop  = DefaultSetting.getProperties(configFilePath);
        for(Map.Entry<Object, Object> entry : prop.entrySet()){
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            sparkConf.set(key, value);
        }
        return sparkConf;
    }

    /**
     * @param configFilePath 설정 파일 위치
     */
    public static Properties getProperties(String configFilePath) {
        Properties prop = new Properties();
        InputStream in = null;
        try {

            if( configFilePath.startsWith("hdfs://")) {
                Path path = new Path(configFilePath);
                FileSystem fs = FileSystem.get(new Configuration());
                prop.load(fs.open(path));
            }else {
                prop.load(new FileInputStream(configFilePath));
            }

        }catch (IOException e){
            System.out.println("Error >>>>> can't read the configFilePath => " + configFilePath);
            System.exit(-1);
        }finally {
            try {
                if (in != null)
                    in.close();
            }catch (IOException e) {}
            finally {
                return prop;
            }
        }
    }

    public static SparkConf getDefaultSparkConfig(String appName){
        /* Setting configuration for spark */
        SparkConf conf = new SparkConf().setAppName(appName).set("spark.rdd.compress", "true")
                .set("spark.shuffle.manager", "hash").set("spark.eventLog.enabled", "true")
                .set("spark.eventLog.compress", "true").set("spark.memory.fraction", "1")
                .set("spark.akka.frameSize", "50")
                .set("spark.hadoop.mapreduce.output.fileoutputformat.compress", "true")
                .set("spark.hadoop.mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec")
                .set("spark.hadoop.mapreduce.output.fileoutputformat.compress.type", "BLOCK")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");


        return conf;
    }
}
