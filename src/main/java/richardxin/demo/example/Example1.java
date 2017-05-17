package richardxin.demo.example;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Example1 {

	public static void main(String[] args) {
		runExample1();

	}
	private static void runExample1() {
		final SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("simple test");
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList(new String[]{"Hadoop", "Spark", "Hive","Kudu","Beeline", "Alluxio","Kylin", "Impala", "Parquet"}));
        JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList(new String[]{"Spark", "Presto", "Impala","Orc", "HDFS", "Tez", "Alluxio","Kafka"}));
        
        JavaPairRDD<String, Integer> names1 = rdd1.mapToPair(item -> new Tuple2<String, Integer>(item, 1));
        JavaPairRDD<String, Integer> names2 = rdd2.mapToPair(item -> new Tuple2<String, Integer>(item,1));
        
        System.out.println(names1.join(names2).collect());
    	System.out.println(names1.leftOuterJoin(names2).collect());
    	System.out.println(names1.rightOuterJoin(names2).collect());
    	
    	sc.stop();
	}

}
