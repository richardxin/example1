package richardxin.demo.example;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class Example2 {
	private static final Pattern PATTERN_COMMA = Pattern.compile(",");
	private static final Pattern PATTERN_COLON = Pattern.compile(":");
	
	public static void main(String[] args) {
		String path;
		if (args.length == 1) 
			path = args[0];
		else 
			path = "data/student_admission.csv";
		
		testSchema1(path);
		

	}
	
	private static void testSchema1(String path) {
		SparkConf sparkConf = new SparkConf().setAppName("Schema Example");
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sparkConf.setMaster("local");		
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
		
		JavaRDD<String> data = sc.textFile(path);
		String schemaString = "admitted,gre,gpa,college_category";
		String schemaString1 = "admitted:int,gre:int,gpa:double,college_category:int";
		
		StructType schema = new StructType(Stream.of(PATTERN_COMMA.split(schemaString)) // or Arrays.stream()
				.map(cn -> DataTypes.createStructField(cn, DataTypes.StringType, true))
				.toArray(size -> new StructField[size])
				);
		
		Stream<String> schemaStr = Stream.of(PATTERN_COMMA.split(schemaString1));
		
		StructType schema1 = new StructType(schemaStr.map(cn -> {			
			String[] pair = PATTERN_COLON.split(cn);
			String k = pair[0];
			String t = pair[1];			
			DataType dt;
			switch (t){
			case "int":				
				dt = DataTypes.IntegerType;
				break;
			case "double":
				dt = DataTypes.DoubleType;
				break;
			case "long":
				dt = DataTypes.LongType;
				break;
			case "date":
				dt = DataTypes.DateType;
				break;
			default:
				dt = DataTypes.StringType;
			}
			return DataTypes.createStructField(k, dt, true);
		}).toArray(size -> new StructField[size]));

		JavaRDD<Row> rddRows = data.map(Example2::parseLine);		
		// Demo: Equivalent to the previous line: using inline lambda expression
		JavaRDD<Row> rddRows1 = data.map(line -> {
			String[] items = PATTERN_COMMA.split(line);
			List<String> values = new ArrayList<>();
			for (int i=0; i<items.length; i++){
				values.add(items[i]);
			}
			return RowFactory.create(values.toArray());
		});
		
		DataFrame df = sqlContext.createDataFrame(rddRows, schema);
		df.show();
		df.printSchema();
		
		schema1.printTreeString();
		sc.stop();
	}
	
	private static Row parseLine(String line){
		 String[] items = PATTERN_COMMA.split(line);
		 List<String> values = new ArrayList<>();
		 for (int i=0; i<items.length; i++){
			 values.add(items[i]);
		 }		 
		 return RowFactory.create(values.toArray());		 
	 }

}
