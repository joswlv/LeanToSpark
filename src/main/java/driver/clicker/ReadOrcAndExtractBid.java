package driver.clicker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;
import udf.DspLogParser;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class ReadOrcAndExtractBid {

	public static void runJob(String period) throws IOException {
		SparkSession spark = SparkSession
				.builder()
				.appName("ReadOrcAndExtractBidFromDsp")
				.getOrCreate();
		spark.sqlContext().udf().register("getValue", new DspLogParser(), DataTypes.StringType);
		List<String> resultDates = getAllDate(period);

		JavaPairRDD<String,String> bidAndCustomCode = null;
		for (String date : resultDates){
			Dataset<Row> ds = spark.read()
					.orc("/user/irteam/addinfra/adxdsp/dt=" + date + "/*/type=c/*");
			ds.createOrReplaceTempView("dsplog"+date);

			spark.sqlContext().sql("select getValue(c3,\'&\',\'dmp\') from dsplog"+date).toJavaRDD()
					.mapToPair((PairFunction<Row, String, String>) row -> new Tuple2<>(row.getString(0),"CST101")).union(bidAndCustomCode);
		}

		try {
			bidAndCustomCode.map((Function<Tuple2<String, String>, String>) tuple2 -> String.format("%s\t%s", tuple2._1(), tuple2._2()))
					.saveAsTextFile("/user/irteam/swjo/clicker/");
		} catch (NullPointerException e) {
			e.printStackTrace();
		}
	}

	public static List<String> getDate(final String period) {
		List<String> resultDate = new ArrayList<>();
		IntStream.range(1, Integer.parseInt(period))
				.forEach(i -> {
					DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
					LocalDateTime periodDate = LocalDateTime.now().minusDays(i);
					resultDate.add(dateFormat.format(periodDate));
				});
		return resultDate;
	}

	public static List<String> getAllDate(final String period) throws IOException {
		List<String> resultDate = new ArrayList<>();
		getDate(period).stream().filter(date -> {
			String src = "/user/irteam/addinfra/adxdsp/dt=" + date + "/*/type=c/*";
			try {
				return FileSystem.get(new Configuration()).exists(new Path(src));
			} catch (IOException e) {
				return false;
			}
		}).forEach(date -> resultDate.add(date));
		return resultDate;
	}

	public static void main(String[] args) throws IOException {
		String period;
		if (args.length == 0) {
			period = "1";
		} else {
			period = Integer.toString(Integer.parseInt(args[0]) + 1);
		}
		runJob(period);
	}
}