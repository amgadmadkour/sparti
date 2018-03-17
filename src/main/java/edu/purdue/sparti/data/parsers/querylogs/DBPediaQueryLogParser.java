package edu.purdue.sparti.data.parsers.querylogs;

/**
 * Created by Amgad Madkour
 */
public class DBPediaQueryLogParser {

	public static void main(String[] args) {

//		Logger.getLogger("org").setLevel(Level.OFF);
//		Logger.getLogger("akka").setLevel(Level.OFF);
//
//		SparkConf sparkConf = new SparkConf().setAppName("SPARTI");
//		JavaSparkContext sc = new JavaSparkContext(sparkConf);
//		NameSpaceHandler RDFNameSpaceHandler = new NameSpaceHandler(RDFDataset.DBPEDIA);
//		TriplesExtractor triplesExtractor = new TriplesExtractor(RDFDataset.DBPEDIA);
//
//		JavaRDD<String> dataSet = sc.textFile(args[0]);
//		JavaRDD<String> reducedLog = dataSet.map(line -> line.split(" ")[0] + "\t" + line.split(" ")[6]);
//		JavaRDD<String> distinctSet = reducedLog.distinct();
//		JavaRDD<String> query = distinctSet.map(line -> new QueryExtractor().extractQuery(line));
//		query.cache();
//		//Process Properties
//		JavaRDD<String> properties = query.map(line -> triplesExtractor.extractProperties(line));
//		JavaRDD<String> filteredProperties = properties.filter(line -> line != null && !line.contains("http___"));
//		//The line below can be used to generate the unique properties
//		//JavaRDD<String> uniqueProperties = filteredProperties.map(line -> StringUtils.join(new HashSet<String>(Arrays.asList(line.split(" ")))," "));
//		filteredProperties.saveAsTextFile(args[1], GzipCodec.class);
//		//Process joins
//		JavaRDD<String> joins = query.map(line -> triplesExtractor.extractJoins(line));
//		JavaRDD<String> filteredJoins = joins.filter(line -> line != null && !line.contains("http___"));
//		filteredJoins.saveAsTextFile(args[2], GzipCodec.class);
	}
}
