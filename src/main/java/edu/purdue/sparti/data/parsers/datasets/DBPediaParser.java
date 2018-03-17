package edu.purdue.sparti.data.parsers.datasets;

import edu.purdue.sparti.model.rdf.RDFDataset;
import edu.purdue.sparti.model.rdf.NameSpaceHandler;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by Amgad Madkour
 */
public class DBPediaParser {

	public static void main(String[] args) {

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		NameSpaceHandler nsHandler = new NameSpaceHandler(RDFDataset.DBPEDIA);

		SparkConf sparkConf = new SparkConf().setAppName("SPARTI");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> dataset = sc.textFile(args[0]);
		JavaRDD<String> initialFilter = dataset.filter(line -> !line.startsWith("#"));
		JavaRDD<String> cleaned = initialFilter.map(line -> line.replaceAll("^\\<", "").replaceAll("\\> \\<", " ").replaceAll(" \\.$", "").replaceAll(">$", ""));
		JavaRDD<String> filteredLines = cleaned.filter(line -> line != null);
		JavaRDD<String> prefix = filteredLines.map(line -> nsHandler.parse(line.split(" ")[0]) + " " + nsHandler.parse(line.split(" ")[1]) + " " + nsHandler.parse(line.split(" ")[2]));
		JavaRDD<String> filteredPrefix = prefix.filter(line -> line != null && !line.startsWith("http___"));
		filteredPrefix.saveAsTextFile(args[1], GzipCodec.class);
	}
}
