package driver.clicker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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

		spark.udf().register("getValue", new DspLogParser(), DataTypes.StringType);
		List<JavaRDD<String>> datasets = getAllDataSet(period, spark);
		JavaPairRDD<String, String> resultData = null;

		for (JavaRDD<String> resultBid : datasets) {
			resultBid.mapToPair(new PairFunction<String, String, String>() {
				@Override
				public Tuple2<String, String> call(String s) throws Exception {
					return new Tuple2<>(s, "CUT102");
				}
			}).union(resultData);
		}
		resultData.map(new Function<Tuple2<String,String>, String>() {
			@Override
			public String call(Tuple2<String, String> tuple2) throws Exception {
				return String.format("%s\t%d", tuple2._1(), tuple2._2());
			}
		}).repartition(5).saveAsTextFile("/user/irteam/swjo/clicker/");

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

	public static List<JavaRDD<String>> getAllDataSet(final String period, SparkSession spark) throws IOException {
		List<JavaRDD<String>> resultJavaRDD = new ArrayList<>();
		getDate(period).stream().filter(date -> {
			String src = "/user/irteam/addinfra/adxdsp/dt=" + date + "/*/type=c";
			try {
				return FileSystem.get(new Configuration()).exists(new Path(src));
			} catch (IOException e) {
				return false;
			}
		}).forEach(date -> {
			Dataset<Row> ds = spark.read()
					.orc("/user/irteam/addinfra/adxdsp/dt=" + date + "/*/type=c");
			ds.createOrReplaceTempView("dsplog");
			resultJavaRDD.add(ds.sqlContext()
					.sql("SELECT getValue(c3,&,dmp) from dsplog")
					.toJavaRDD().map(new Function<Row, String>() {
						@Override
						public String call(Row row) throws Exception {
							return row.toString();
						}
					})
				);
		});
		return resultJavaRDD;
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
