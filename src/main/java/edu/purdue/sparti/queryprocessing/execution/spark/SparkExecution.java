package edu.purdue.sparti.queryprocessing.execution.spark;

import edu.purdue.sparti.model.rdf.NameSpaceHandler;
import edu.purdue.sparti.model.rdf.TriplesExtractor;
import edu.purdue.sparti.metadata.SpartiMetadata;
import edu.purdue.sparti.model.rdf.RDFDataset;
import edu.purdue.sparti.queryprocessing.execution.SpartiExecution;
import edu.purdue.sparti.queryprocessing.translation.SPARQLQueryModifier;
import edu.purdue.sparti.queryprocessing.translation.SQLQueryModifier;
import edu.purdue.sparti.queryprocessing.translation.SpartiTranslator;
import edu.purdue.sparti.utils.Permute;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.apache.spark.sql.functions.col;


/**
 * @author Amgad Madkour, adopted from S2RDF (applies to most of the project)
 */
public class SparkExecution implements Serializable, SpartiExecution {

	private static Logger LOG = Logger.getLogger(SparkExecution.class.getName());

	private static JavaSparkContext sc;
	private static SQLContext sqlContext;
	private static SpartiMetadata metadata;

	public SparkExecution(SpartiMetadata metadata) {
		SparkExecution.metadata = metadata;
		Logger.getLogger(LOG.getName()).setLevel(Level.DEBUG);
		SparkConf conf = new SparkConf()
				.setAppName("SpartiExecutor")
				.set("spark.sql.parquet.cacheMetadata", "true")
				.set("spark.executor.memory", "20g")
				.set("spark.sql.inMemoryColumnarStorage.compressed", "true")
				.set("spark.sql.autoBroadcastJoinThreshold", "-1")
				.set("spark.sql.parquet.filterPushdown", "true")
				.set("spark.sql.inMemoryColumnarStorage.batchSize", "20000")
				.set("spark.sql.shuffle.partitions", "50");
		//.set("spark.driver.allowMultipleContexts", "true")
		//.set("spark.sql.parquet.mergeSchema", "true");
		sc = new JavaSparkContext(conf);
		sqlContext = new SQLContext(sc);
		Logger.getLogger(LOG.getName()).setLevel(Level.INFO);
	}

	public boolean executeQueries(String queriesPath) {

		try {

			PrintWriter printer = new PrintWriter(new FileWriter(metadata.getLocalDbPath() + "results-sparti.txt"));
			PrintWriter printerIO = new PrintWriter(new FileWriter(metadata.getLocalDbPath() + "resultsIO-sparti.txt"));

			File folder = new File(queriesPath);
			File[] listOfFiles = folder.listFiles();
			ArrayList<String> queries = new ArrayList<>();

			Arrays.sort(listOfFiles);

			for (int i = 0; i < listOfFiles.length; i++) {
				if (listOfFiles[i].isFile()) {
					queries.add(listOfFiles[i].getName());
				}
			}

			ArrayList<String> cachedTables = new ArrayList<>();
			int numQueries = 0;
			boolean firstTime = true;

			for (String qryName : queries) {

				List<Map> tablesStats = new ArrayList<>();
				long totalCount = 0;
				long totalCache = 0;

				//Uncache tables
				for (String cachedTable : cachedTables) {
					double start = System.currentTimeMillis();
					sqlContext.dropTempTable(cachedTable);
					double time = System.currentTimeMillis() - start;
//					LOG.debug("\t\tUncached " + cachedTable + " in " + time + "ms");
				}
				cachedTables.clear();

				LOG.debug(">>>> QUERY NAME : " + qryName);
				String qry = new String(Files.readAllBytes(Paths.get(metadata.getQueryFilePath() + qryName)));
				numQueries += 1;

				if (qry.contains("BASE ")) {
					SPARQLQueryModifier sqm = new SPARQLQueryModifier(new NameSpaceHandler(metadata.getBenchmarkName()));
					qry = sqm.convertBasePrefix(qry);
				}

				TriplesExtractor extractor = new TriplesExtractor(metadata.getBenchmarkName());
				extractor.extractByID(qry);

				ArrayList<String> queryProperties = extractor.getProperties();
				HashMap<String, ArrayList<String>> queryJoins = extractor.getJoinEntries();
				HashMap<String, String> bgpReplacements = extractor.getBgpReplacement();

				qry = qry.replaceAll("(\\t| )+", " ");
				for (Map.Entry<String, String> entry : bgpReplacements.entrySet()) {
					qry = qry.replace(entry.getKey(), entry.getValue());
				}

				LOG.debug("SPARQL:\n" + qry);

				LOG.debug("\tProperties :");
				queryProperties.forEach(LOG::debug);
				LOG.debug("\tJoins :");
				queryJoins.entrySet().forEach(LOG::debug);
				LOG.debug("\tBGP Replacements :");
				bgpReplacements.entrySet().forEach(LOG::debug);

				//Add patterns extracted from the query to the learning data
				metadata.addQueryJoinPatterns(queryJoins);
				metadata.addQueryPropertiesPattern(queryProperties);

				HashMap<String, String> propTableNames = new HashMap<>();

				boolean spartiTable;
				for (String propWithID : queryProperties) {
					spartiTable = false;
					//Determine the table name for a property (can be a VP table or a Sparti table)
					String propName = propWithID.split("\\_\\d+")[0];
					String tableName = metadata.getTableName(propName);

					if (!tableName.equals(propName)) {
						propTableNames.put(propName, tableName);
						spartiTable = true;
					}


					//Caching
					if (!cachedTables.contains(propWithID)) {
						String semfilter;
						DataFrame table;
						if (spartiTable) {
//							LOG.debug("\tLoading SPARTI Table "+tableName);
							semfilter = getSemanticFilter(propWithID, propTableNames, queryJoins);
							if (semfilter != null) {
								table = sqlContext.parquetFile(metadata.getHdfsDbPath() + tableName)
										.select(col("sub"), col("obj"), col(semfilter)).filter(col(semfilter).isNotNull()).select(col("sub"), col("obj"));
//								LOG.debug("\t\tUSING SEMANTIC FILTER : "+semfilter);
								int numTup = metadata.getSemanticColumnCount(propTableNames.get(propName), semfilter);
								HashMap<String, String> entry = new HashMap<>();
								entry.put("tableName", propWithID);
								entry.put("numTuples", String.valueOf(numTup));
								entry.put("numTriples", String.valueOf(metadata.getDbInfo().get("numTriples")));
								entry.put("ratio", String.valueOf(0));
								tablesStats.add(entry);
//								LOG.debug("\t\tSTAT COUNT: "+ numTup);
							} else {
								table = sqlContext.parquetFile(metadata.getHdfsDbPath() + tableName)
										.select(col("sub"), col("obj"));
								HashMap<String, String> entry = new HashMap<>();
								entry.put("tableName", propWithID);
								entry.put("numTuples", String.valueOf(metadata.getSpartiTableInfo(tableName).get("numTuples")));
								entry.put("numTriples", String.valueOf(metadata.getDbInfo().get("numTriples")));
								entry.put("ratio", String.valueOf(metadata.getSpartiTableInfo(tableName).get("ratio")));
								tablesStats.add(entry);
//								LOG.debug("\t\tSTAT COUNT: "+ String.valueOf(metadata.getSpartiTableInfo(tableName).get("numTuples")));
							}
						} else {
//							LOG.debug("\tLoading NON-SPARTI TABLE "+ tableName);
							table = sqlContext.parquetFile(metadata.getHdfsDbPath() + tableName);
							HashMap<String, String> entry = new HashMap<>();
							entry.put("tableName", propWithID);
							entry.put("numTuples", String.valueOf(metadata.getVpTableInfo(tableName).get("numTuples")));
							entry.put("numTriples", String.valueOf(metadata.getDbInfo().get("numTriples")));
							entry.put("ratio", String.valueOf(metadata.getVpTableInfo(tableName).get("ratio")));
							tablesStats.add(entry);
//							LOG.debug("\t\tSTAT COUNT: "+String.valueOf(metadata.getVpTableInfo(tableName).get("numTuples")));
						}
						table.registerTempTable(propWithID);
						sqlContext.cacheTable(propWithID);
						//Calculate caching time
						double start = System.currentTimeMillis();
						long size = table.count();
						double time = System.currentTimeMillis() - start;
//						LOG.debug("\t\tCached the registered table " + propWithID + "(" + size + ") in " + time + "ms");
						totalCount += size;
						totalCache += time;
						cachedTables.add(propWithID);
					}
				}

				SpartiTranslator translator = new SpartiTranslator(tablesStats);
				String sqlQuery = translator.translate(qry);
				//LOG.debug("BEFORE: \n" + sqlQuery);
				SQLQueryModifier modifier = new SQLQueryModifier(sqlQuery, metadata);
				sqlQuery = modifier.convertVPToSparti(propTableNames, queryJoins);

				//PrintWriter ptr = new PrintWriter(new FileWriter(metadata.getLocalDbPath() + "/SQL/" + qryName));
				//ptr.println(sqlQuery);
				//ptr.close();

				sqlQuery = sqlQuery.replaceAll("\\<", "").replaceAll("\\>", "").replaceAll("http\\_\\_", "http:");
				sqlQuery = sqlQuery.replaceAll("\\:", "__");
				LOG.debug("AFTER: \n" + sqlQuery);

				if (firstTime) {
					DataFrame temp = sqlContext.sql(sqlQuery);
					temp.count();
					firstTime = false;
				}

				double start = System.currentTimeMillis();
				DataFrame temp = sqlContext.sql(sqlQuery);
				long resSize = temp.count();
				double time = System.currentTimeMillis() - start;
				temp = null;

				printer.println(qryName + "\t" + resSize + "\t" + time);
				printerIO.println(qryName + "\t" + totalCount + "\t" + totalCache);

//				LOG.debug("######################################");
				LOG.info("QUERY: " + qryName);
				LOG.info("Time: " + time + "ms (" + resSize + ")");
//				LOG.debug("######################################");
				//temp.show();
			}
//			LOG.debug("Executed " + numQueries + " queries successfully");
			printer.close();
			printerIO.close();
		} catch (IOException exp) {
			exp.printStackTrace();
			return false;
		}
		return true;
	}

	private String getSemanticFilter(String queryProperty, HashMap<String, String> newPropTableNames, HashMap<String, ArrayList<String>> queryJoins) {

		//Data Preparation
		String realProperty = queryProperty.split("\\_\\d+")[0];
		ArrayList<ArrayList<String>> matchingJoins = new ArrayList<>();
		for (Map.Entry<String, ArrayList<String>> entry : queryJoins.entrySet()) {
			if (entry.getValue().contains(queryProperty + "_TRPS") || entry.getValue().contains(queryProperty + "_TRPO")) {
				ArrayList<String> jnProps = new ArrayList<>();
				for (String val : entry.getValue()) {
					jnProps.add(val.replaceAll("\\_\\d+", ""));
				}
				matchingJoins.add(jnProps);
			}
		}

		HashSet<String> candidate = new HashSet<>();
		//Get all permutations of the join in order to determine the appropriate column names
		for (ArrayList<String> entry : matchingJoins) {
			HashSet<String> freqJoins = new HashSet<>();
			List<List<String>> allcombine = new ArrayList<>();
			for (int i = 0; i < entry.size(); i++) {
				for (int j = i + 1; j < entry.size(); j++) {
					List<String> combinations = new ArrayList<>();
					combinations.add(entry.get(i));
					combinations.add(entry.get(j));
					allcombine.add(combinations);
				}
			}
			if (entry.size() > 2)
				allcombine.add(entry);
			for (List candid : allcombine) {
				Permute perm = new Permute();
				List<List<String>> joinColumns = perm.permute(candid, 0);
				for (List<String> combination : joinColumns) {
					if (combination.get(0).startsWith(realProperty))
						freqJoins.add(StringUtils.join(combination, "_JOIN_"));
				}
			}

			for (String joinName : freqJoins) {
				String[] joinCond = joinName.split("\\_JOIN\\_");
				ArrayList<String> tableJoins = new ArrayList<>();
				for (String p : joinCond) {
					tableJoins.add(p.split("\\_TRP")[0]);
				}
				Map mp = metadata.getSpartiTableInfo(newPropTableNames.get(realProperty));
				String schema = (String) mp.get("partitionProperties");
				String[] subTables = schema.split(",");
				int matchingEntries = 0;
				for (String jn : tableJoins) {
					if (Arrays.asList(subTables).contains(jn)) {
						matchingEntries += 1;
					}
				}
				if (matchingEntries <= tableJoins.size()) {
					candidate.add(joinName);
				}
			}
		}
		//Determine which candidate will be used to read in the data based on
		//the number of entries it contains (the smaller the better)
		long minCount = Long.MAX_VALUE;
		String minCombination = null;
		for (String cand : candidate) {
			long count = metadata.getSemanticColumnCount(newPropTableNames.get(realProperty), cand); //TODO maybe get the schema first better ?
			if (count <= 0) //Does not exist
				continue;
			if (count < minCount) {
				minCount = count;
				minCombination = cand;
			}
		}

		return minCombination;
	}



	@Override
	public boolean execute(String queryPath, String localdbPath, String hdfsdbPath, RDFDataset RDFDatasetName) {

		String qry;
		try {

//			SparkSemanticPartitioning sparti = new SparkSemanticPartitioning(sqlContext, sc, metadata);

			BufferedReader rdr = new BufferedReader(new FileReader(queryPath));
			SpartiTranslator translator = new SpartiTranslator(metadata.getVpTablesList());
			ArrayList<String> cachedTables = new ArrayList<>();
			int numQueries = 0;

			while ((qry = rdr.readLine()) != null) {

				numQueries += 1;

//				if (numQueries == 2) {
//					sparti.runFPGrowthApproach();
//				}

				TriplesExtractor extractor = new TriplesExtractor(metadata.getBenchmarkName());

				extractor.extractByID(qry);
				ArrayList<String> queryProperties = extractor.getProperties();
				HashMap<String, ArrayList<String>> queryJoins = extractor.getJoinEntries();

//				LOG.debug("Properties : \n" + queryProperties);
//				LOG.debug("Joins : \n" + queryJoins);

				//Add patterns extracted from the query to the learning data
				metadata.addQueryJoinPatterns(queryJoins);
				metadata.addQueryPropertiesPattern(queryProperties);

				HashMap<String, String> propTableNames = new HashMap<>();

				for (String prop : queryProperties) {
					//Determine the table name for a property (can be a VP table or a Sparti table)
					String tableName = metadata.getTableName(prop);

					if (!tableName.equals(prop))
						propTableNames.put(prop, tableName);

					//Caching
					if (!cachedTables.contains(tableName)) {
						DataFrame table = sqlContext.parquetFile(metadata.getHdfsDbPath() + tableName);
						table.registerTempTable(tableName);
						sqlContext.cacheTable(tableName);
						//Load Statistics
						double start = System.currentTimeMillis();
						long size = table.count();
						double time = System.currentTimeMillis() - start;
						LOG.debug("\t\tCached " + size + " Elements in " + time + "ms");
						cachedTables.add(tableName);
					}
				}

				String sqlQuery = translator.translate(qry);
				//LOG.debug("BEFORE: \n" + sqlQuery);
				SQLQueryModifier modifier = new SQLQueryModifier(sqlQuery, metadata);
				sqlQuery = modifier.convertVPToSparti(propTableNames, queryJoins);
				//LOG.debug("AFTER: \n" + sqlQuery);

				double start = System.currentTimeMillis();
				DataFrame temp = sqlContext.sql(sqlQuery);
				long resSize = temp.count();
				double time = System.currentTimeMillis() - start;
//				LOG.debug("Query Execution Time: " + time + "ms (" + resSize + ")");
				temp.show();
			}
		} catch (IOException exp) {
			exp.printStackTrace();
			return false;
		}
		return true;
	}
}
