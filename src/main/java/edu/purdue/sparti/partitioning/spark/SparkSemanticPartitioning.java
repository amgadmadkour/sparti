package edu.purdue.sparti.partitioning.spark;

import edu.purdue.sparti.metadata.SpartiMetadata;
import edu.purdue.sparti.model.rdf.NameSpaceHandler;
import edu.purdue.sparti.model.rdf.TriplesExtractor;
import edu.purdue.sparti.queryprocessing.translation.SPARQLQueryModifier;
import edu.purdue.sparti.utils.MapUtils;
import edu.purdue.sparti.utils.Permute;
import orestes.bloomfilter.BloomFilter;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.deeplearning4j.models.embeddings.learning.impl.elements.CBOW;
import org.deeplearning4j.models.glove.Glove;
import org.deeplearning4j.models.word2vec.VocabWord;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.text.sentenceiterator.CollectionSentenceIterator;
import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;

/**
 * @author Amgad Madkour
 */
public class SparkSemanticPartitioning implements Serializable {

	private static final Logger LOG = Logger.getLogger(SparkSemanticPartitioning.class.getName());
	private static SQLContext sqlContext;
	private static JavaSparkContext sc;

	private Word2Vec vec;
	private SpartiMetadata metadata;
	private double threshold;

	public Word2Vec getVector() {
		return this.vec;
	}

	public double getThreshold() {
		return threshold;
	}

	public void setThreshold(double threshold) {
		this.threshold = threshold;
	}

	public SparkSemanticPartitioning(SpartiMetadata md) {
		this.threshold = 0.5;
		this.metadata = md;
		SparkConf conf = new SparkConf()
				.setAppName("SpartiPartitioner")
				.set("spark.sql.parquet.filterPushdown", "true")
				.set("spark.network.timeout", "3000")
				.set("spark.driver.maxResultSize", "20g")
				.set("spark.sql.parquet.cacheMetadata", "true")
				.set("spark.rpc.askTimeout", "3000")
				.set("spark.rpc.lookupTimeout", "3000")
				.set("spark.core.connection.ack.wait.timeout", "3000")
				.set("spark.core.connection.auth.wait.timeout", "3000")
				.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC");

		sc = new JavaSparkContext(conf);
		sqlContext = new SQLContext(sc);
		Logger.getLogger(LOG.getName()).setLevel(Level.DEBUG);
	}

	public SparkSemanticPartitioning(SQLContext sqlContext, JavaSparkContext sc, SpartiMetadata metadata) {
		this.metadata = metadata;
		SparkSemanticPartitioning.sc = sc;
		SparkSemanticPartitioning.sqlContext = sqlContext;
		Logger.getLogger(LOG.getName()).setLevel(Level.DEBUG);
	}

	public void createPartitions(HashMap<String, Set<String>> proposedPartitions) {

//		LOG.debug("QUERY JOIN PATTERNS : ");
//		this.metadata.getUniqueQueryJoinPatterns().forEach(LOG::debug);

		Map freqJoinPatterns = runFPGrowth(this.metadata.getUniqueQueryJoinPatterns(), 0);

		//sortedJoins.entrySet().forEach(LOG::debug);

		int counter = 0;
		//For each partition proposal, create the appropriate table
		for (Map.Entry<String, Set<String>> partition : proposedPartitions.entrySet()) {
			counter += 1;
			String spartiProperty = partition.getKey();
			List<String> partitionProperties = new ArrayList(partition.getValue());

			LOG.info(">> Processing Partition (" + counter + "/" + proposedPartitions.size() + ")" + " for " + spartiProperty + " " + partitionProperties);

			HashMap<String, HashMap<String, BloomFilter>> filters = new HashMap<>();

			for (String p : partitionProperties) {
				HashMap<String, BloomFilter> filter = new HashMap<>();
				filter.put("sub", metadata.getVPBloomFilter(p, "sub"));
				filter.put("obj", metadata.getVPBloomFilter(p, "obj"));
				filters.put(p, filter);
			}

			Broadcast<HashMap<String, HashMap<String, BloomFilter>>> bf = sc.broadcast(filters);

			String newTable = "SPARTI_" + spartiProperty;

			if (!this.metadata.hasSpartiTable(newTable)) {
				ArrayList<String> matches = getMatchingJoins(freqJoinPatterns, partitionProperties, spartiProperty);
				ArrayList<String> matchingJoins = sortMatchesBySize(matches);
				LOG.debug("Reading property file " + spartiProperty);
				DataFrame frame = sqlContext.read().parquet(this.metadata.getHdfsDbPath() + spartiProperty);

				frame = addSemanticColumns(spartiProperty, newTable, frame, matchingJoins, partitionProperties, bf);

				frame.cache();
				String[] fields = frame.schema().fieldNames();
				long size = frame.count();
				this.metadata.addSpartiPartition(newTable, size, frame.schema().fieldNames(), partitionProperties);
				for (String f : fields) {
					if (f.equals("sub") || f.equals("obj"))
						continue;
//					LOG.debug("Calculating statistics : " + f);
					long resultCount = frame.select(f).where(f + " IS NOT NULL").count();
					this.metadata.incrementSemanticColumnCount(newTable, f, resultCount);
				}

				LOG.debug("Writing to file");
				frame.write().mode(SaveMode.Overwrite).parquet(this.metadata.getHdfsDbPath() + newTable);
				//movePartitions(properties);
				LOG.debug("SPARTI Table computed succesfully");
			}

			bf.destroy();

			//Create the property table by (outer) joining data frames [append all frames to the first]

//				for (int i = 1; i < frames.length; i++) {
//					frames[0] = frames[0].join(frames[i],
//							frames[0].col("sub").equalTo(frames[i].col("sub")), "outer")
//							.withColumn("sub_", coalesce(frames[0].col("sub"), frames[i].col("sub")))
//							.drop("sub")
//							.withColumnRenamed("sub_", "sub");
//				}
//				int size = (int) frames[0].count();
//				this.metadata.addSpartiPartition(newTable, properties, size, frames[0].schema().fieldNames());
		}
		this.metadata.syncMetadata();
	}

	private ArrayList<String> sortMatchesBySize(ArrayList<String> joins) {
		Map<String, Integer> result = new HashMap<>();
		ArrayList<String> sortedMatches = new ArrayList<>();
		for (String entry : joins) {
			result.put(entry, entry.split("\\_JOIN\\_").length);
		}
		Map<String, Integer> res = MapUtils.sortByValue(result, "asc");
		for (Map.Entry<String, Integer> entry : res.entrySet()) {
			sortedMatches.add(entry.getKey());
		}
		return sortedMatches;
	}

	private void movePartitions(List<String> partitions) {

		try {
			Configuration configuration = new Configuration();
			FileSystem fs = FileSystem.get(new URI("hdfs://172.18.11.205:8020"), configuration);
			if (!fs.exists(new Path(this.metadata.getHdfsDbPath() + "tmp")))
				fs.mkdirs(new Path(this.metadata.getHdfsDbPath() + "tmp"));
			for (String partition : partitions) {
				fs.rename(new Path(this.metadata.getHdfsDbPath() + partition), new
						Path(this.metadata.getHdfsDbPath() + "tmp/" + partition));
				this.metadata.removeVP(partition);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}

	private DataFrame addSemanticColumns(String property, String newTable, DataFrame table, ArrayList<String> queries, List<String> partitions, Broadcast<HashMap<String, HashMap<String, BloomFilter>>> bf) {

		ArrayList<String> newColumns = new ArrayList<>();
		int counter = 0;
		int maxColumns = 100;
		for (int m = 0; m < queries.size(); m++) {

			if (counter == maxColumns)
				break;

			List<String> patt = new LinkedList<>(Arrays.asList(queries.get(m).split("\\_JOIN\\_")));
			String propSub = property + "_TRPS";
			String propObj = property + "_TRPO";
			if (patt.contains(propSub)) {
				patt.remove(propSub);
				patt.add(0, propSub);
			} else {
				patt.remove(propObj);
				patt.add(0, propObj);
			}
			String columnName = StringUtils.join(patt, "_JOIN_");

//			HashSet<String> freqJoins = getPermutations(queries.get(m));
//			HashSet<String> filtered = new HashSet<>();
//			for (String fj : freqJoins) {
//				if (fj.startsWith(property))
//					filtered.add(fj);
//			}

			List<String> existingColumns = Arrays.asList(table.schema().fieldNames());
			boolean existingColumn = false;
			for (String ext : existingColumns) {
				if (compareArrays(ext.split("\\_JOIN\\_"), patt.toArray(new String[patt.size()]))) {
					existingColumn = true;
					break;
					}
				}
			//Make sure we add unique column names
			if (!existingColumn) {
				counter += 1;
//				LOG.debug("Including column (" + counter + ") : " + columnName);
				newColumns.add(columnName);
				table = table.withColumn(columnName, lit(null).cast(DataTypes.BooleanType));
			}
		}
		table = computeBloomJoin(table, newColumns, bf);

//			LOG.debug("Two properties only, using predefined combinations ..");
//
//			ArrayList<String> predefinedQueries = new ArrayList<>();
//			String part1 = partitions.get(0);
//			String part2 = partitions.get(1);
//			predefinedQueries.add(part1 + "_TRPS_JOIN_" + part2 + "_TRPS");
//			predefinedQueries.add(part1 + "_TRPS_JOIN_" + part2 + "_TRPO");
//			predefinedQueries.add(part1 + "_TRPO_JOIN_" + part2 + "_TRPS");
//			for (int m = 0; m < predefinedQueries.size(); m++) {
//				HashSet<String> freqJoins = getPermutations(predefinedQueries.get(m));
//				HashSet<String> filtered = new HashSet<>();
//				for (String fj : freqJoins) {
//					if (fj.startsWith(property))
//						filtered.add(fj);
//				}
//				List<String> existingColumns = Arrays.asList(table.schema().fieldNames());
//				ArrayList<String> newColumns = new ArrayList<>();
//
//				for (String columnName : filtered) {
//					//Make sure we add unique column names
//					if (!existingColumns.contains(columnName)) {
//						LOG.debug("Including join : " + columnName);
//						newColumns.add(columnName);
//						this.metadata.addSpartiColumn(newTable, columnName);
//						table = table.withColumn(columnName, lit(null).cast(DataTypes.BooleanType));
//					}
//				}
//				table = computeBloomJoin(table, newColumns, bf);
//			}

		return table;
	}

	public static boolean compareArrays(String[] arr1, String[] arr2) {
		HashSet<String> set1 = new HashSet<String>(Arrays.asList(arr1));
		HashSet<String> set2 = new HashSet<String>(Arrays.asList(arr2));
		return set1.equals(set2);
	}

	private DataFrame computeBloomJoin(DataFrame table, ArrayList<String> newColumns, Broadcast<HashMap<String, HashMap<String, BloomFilter>>> bf) {

		JavaRDD<Row> rdd = table.toJavaRDD();
		JavaRDD<Row> res = rdd.map(r -> {

			String[] fieldNames = r.schema().fieldNames();
			Object[] elems = new Object[fieldNames.length];

			//Iterate over each field
			for (int i = 0; i < fieldNames.length; i++) {
				//For each field, if its one of the new columns, then compute its bloom join
				boolean matchBloomField = false;
				for (int l = 0; l < newColumns.size(); l++) {
					if (newColumns.get(l).equals(fieldNames[i])) {
						matchBloomField = true;
						//Compute the Bloom join
						String[] tableAndJoin = fieldNames[i].split("\\_JOIN\\_");
						Map<String, String> mapTableAndJoin = new HashMap<>();
						//Join based on size
						ArrayList<String> tablesList = new ArrayList<>();
						for (int j = 0; j < tableAndJoin.length; j++) {
							String join = null;
							String tableName = null;
							if (tableAndJoin[j].contains("TRPS")) {
								String[] parts = tableAndJoin[j].split("\\_TRPS");
								tableName = parts[0];
								join = "sub";
							} else if (tableAndJoin[j].contains("TRPO")) {
								String[] parts = tableAndJoin[j].split("\\_TRPO");
								tableName = parts[0];
								join = "obj";
							}
							mapTableAndJoin.put(tableName, join);
							tablesList.add(tableName);
						}

						String leftTable = mapTableAndJoin.get(tablesList.get(0));
						boolean notPresent = false;
						//Iterate over all remaining properties
						for (int k = 1; k < tablesList.size(); k++) {
							String rightTable = tablesList.get(k);
							//JsonElement jsonElem = new JsonParser().parse(bf.value().get(rightTable).get(mapTableAndJoin.get(rightTable)));
							//BloomFilter filter = BloomFilterConverter.fromJson(jsonElem);
							BloomFilter filter = bf.value().get(rightTable).get(mapTableAndJoin.get(rightTable));
							BloomFilter bloomFilterRight = filter;
							String leftTableColumnVal = r.getString(r.fieldIndex(leftTable));

							if (bloomFilterRight.contains(leftTableColumnVal)) {
								continue;
							}
							notPresent = true;
							break;
						}
						if (notPresent) {
							elems[i] = null;
						} else {
							elems[i] = true;
						}
					}
				}
				if (!matchBloomField)
					elems[i] = r.get(i);
			}
			return RowFactory.create(elems);
		});
		table = sqlContext.createDataFrame(res, table.schema());
		return table;
	}

	private HashSet<String> getPermutations(String query) {
		HashSet<String> freqJoins = new HashSet<>();
		String[] qparts = query.split("\\_JOIN\\_");
		List<List<String>> allcombine = new ArrayList<>();
		for (int i = 0; i < qparts.length; i++) {
			for (int j = i + 1; j < qparts.length; j++) {
				List<String> combinations = new ArrayList<>();
				combinations.add(qparts[i]);
				combinations.add(qparts[j]);
				allcombine.add(combinations);
			}
		}
		List<String> all = Arrays.asList(qparts);
		allcombine.add(all);
		for (List candid : allcombine) {
			Permute perm = new Permute();
			List<List<String>> joinColumns = perm.permute(candid, 0);
			for (List combination : joinColumns) {
				freqJoins.add(StringUtils.join(combination, "_JOIN_"));
			}
		}
		return freqJoins;
	}

	private ArrayList<String> getMatchingJoins(Map<Set<String>, Long> joins, List<String> partitions, String property) {

		HashSet<String> queryJoins = new HashSet<>();
		//int maxNumCols = 50;
		//int currColumns = 0;

		for (Map.Entry<Set<String>, Long> entry : joins.entrySet()) {

//			if(queryJoins.size()>maxNumCols)
//				break;

			Set<String> join = entry.getKey();
			HashMap<String, String> joinProp = new HashMap<>();
			boolean containsProperty = false;
			for (String jn : join) {
				String prop = jn.split("\\_TRP")[0];
				if (prop.equals(property))
					containsProperty = true;
				joinProp.put(prop, jn);
			}
			//Extract properties from the join
			ArrayList<String> validEntries = new ArrayList<>();
			for (String partition : partitions) {
				if (joinProp.containsKey(partition)) {
					validEntries.add(joinProp.get(partition));
				}
			}
			if (validEntries.size() >= 2 && containsProperty && validEntries.size() <= partitions.size()) {
//				long combinations = MathUtils.factorial(validEntries.size());
//				if ((combinations + currColumns) <= maxNumCols) {
//					currColumns += combinations;
				Arrays.sort(validEntries.toArray());
				queryJoins.add(StringUtils.join(validEntries, "_JOIN_"));
//				}
			}
		}

//		if (validEntries.size() >= 2) {
//			//Generate all permutations for this pattern
//			Permute perm = new Permute();
//			List<List<String>> joinColumns = perm.permute(new ArrayList<>(validEntries), 0);
//			for (List combination : joinColumns) {
//				queryJoins.add(StringUtils.join(combination, "_JOIN_"));
//			}
//		}

		//queryJoins.add(StringUtils.join(validEntries, "_JOIN_"));
		//Put into an array to access queries by index
		ArrayList<String> queries = new ArrayList<>();
		queries.addAll(queryJoins);
//		LOG.debug("----------------------------");
//		LOG.debug("Partition : " + partitions);
//		LOG.debug("Matching Queries : " + queries + "-- Size : " + queries.size());
//		LOG.debug("----------------------------");

		//****************TEST CASE*******************
//		ArrayList<String> queries = new ArrayList<>();
//		queries.add("dbr__mention_TRPS_JOIN_dbr__tweet_TRPS_JOIN_dbr__friends_TRPS");
//		queries.add("dbr__tweet_TRPS_JOIN_dbr__mention_TRPS");
		//********************************************

		return queries;
	}

	public HashMap<String, Set<String>> runFPGrowthApproach(ArrayList<String> properties) {

		HashMap<String, Set<String>> partitions = new HashMap<>();
		if (properties.size() > 1) {
			//Get most frequent properties
			Map<String, Integer> sortedByFreq = sortPropertiesByFrequency(properties);
			ArrayList<String> filteredProperties = new ArrayList<>();
			for (Map.Entry<String, Integer> entry : sortedByFreq.entrySet()) {
				if (entry.getValue() > 0)
					filteredProperties.add(entry.getKey());
			}
			//LOG.debug("Most frequent properties : " + filteredProperties.size());

			partitions = getFPGrowthPartitions(filteredProperties);

//			LOG.debug(">>>> Proposed Partitions:");
//			partitions.entrySet().forEach(LOG::debug);
		}
		return partitions;
	}

	public HashMap<String, Set<String>> runWordEmbeddingApproach(ArrayList<String> properties) {
		HashMap<String, Set<String>> partitions = new HashMap<>();
		if (properties.size() > 1) {
			//Get most frequent properties
			Map<String, Integer> sortedByFreq = sortPropertiesByFrequency(properties);
			ArrayList<String> filteredProperties = new ArrayList<>();
			for (Map.Entry<String, Integer> entry : sortedByFreq.entrySet()) {
				if (entry.getValue() > 0)
					filteredProperties.add(entry.getKey());
			}
			//LOG.debug("Most frequent properties : " + filteredProperties.size());

			//Start the Learning process
			for (String prop : properties) {
				HashSet res = new HashSet<>();
				res.addAll(this.vec.wordsNearest(prop, 10));
				if (res.size() > 0) {
					res.add(prop.toLowerCase());
					partitions.put(prop, res);
				}
			}

//			LOG.debug(">>>> Proposed Partitions:");
//			partitions.entrySet().forEach(LOG::debug);
		}
		return partitions;
	}

	public void parseQueryDataset() {

		LOG.info("Parsing " + this.metadata.getBenchmarkName() + " dataset");
		try {
			BufferedReader rdr = new BufferedReader(new FileReader(this.metadata.getQueryFilePath()));
			String qry;
			long counter = 0;

			while ((qry = rdr.readLine()) != null) {

				SPARQLQueryModifier modifier = new SPARQLQueryModifier(new NameSpaceHandler(metadata.getBenchmarkName()));
				qry = modifier.convertBasePrefix(qry);

				counter += 1;
				TriplesExtractor extractor = new TriplesExtractor(this.metadata.getBenchmarkName());
				extractor.extract(qry);
				ArrayList<String> queryProperties = extractor.getProperties();
				HashMap<String, ArrayList<String>> queryJoins = extractor.getJoinEntries();
				//Add patterns extracted from the query to the learning data
				this.metadata.addQueryJoinPatterns(queryJoins);
				this.metadata.addQueryPropertiesPattern(queryProperties);
			}
			LOG.info("Number of Queries Parsed : " + counter);
			this.metadata.setNumQueries(counter);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private HashMap<String, Set<String>> getFPGrowthPartitions(ArrayList<String> properties) {

		HashMap<String, Set<String>> result = new HashMap<>();

		//FPGrowth
		HashMap<Set<String>, Long> freqPropertiesPatterns = runFPGrowth(this.metadata.getUniqueQueryJoinPatterns(), 0);

		HashMap<String, HashMap<Set<String>, Long>> propPatterns = new HashMap<>();

		//Generate property-pattern association
		for (Map.Entry<Set<String>, Long> pattern : freqPropertiesPatterns.entrySet()) {
			Set<String> props = pattern.getKey();
			for (String p : props) {
				if (propPatterns.containsKey(p)) {
					propPatterns.get(p).put(pattern.getKey(), pattern.getValue());
				} else {
					HashMap<Set<String>, Long> ents = new HashMap<>();
					ents.put(pattern.getKey(), pattern.getValue());
					propPatterns.put(p, ents);
				}
			}
		}

		for (Map.Entry<String, HashMap<Set<String>, Long>> entry : propPatterns.entrySet()) {
			String propertyName = entry.getKey();
			HashMap<Set<String>, Long> patterns = entry.getValue();
			//LOG.debug("=== EVALUATING "+propertyName+" =======");
			Collection<String> res = costModel(patterns, properties.size());
			result.put(cleanup(propertyName), cleanup(res));
		}


		//OLD  CODE

//		while (sortedProperties.size() > 0) {
//			System.out.println("Evaluating : " + sortedProperties.get(0));
//			String headProperty = sortedProperties.get(0);
//			Set<String> res = costModel(propPatterns.get(headProperty), freqPropertiesPatterns, sortedProperties, properties.size());
//			sortedProperties.remove(0);
//			if (res != null) {
//				for (String prop : res) {
//					if (sortedProperties.contains(prop)) {
//						System.out.println("------ Removing " + prop);
//						sortedProperties.remove(prop);
//					}
//				}
//				result.put(StringUtils.join(res, "_JOIN_"), res);
//			}
//		}
//


//		LOG.info("Proposals : ");
//		result.values().forEach(LOG::debug);
//		LOG.info("\tNumber of Proposals : " + result.values().size());
//		HashSet<String> numProperties = new HashSet<>();
//		result.values().forEach(numProperties::addAll);
//		LOG.info("\tNumber of Properties in Proposals : " + numProperties.size());

//		//TODO Send the joins ordered by size (smallest to largest)
//		ArrayList<String> sample = new ArrayList<>();
//		sample.add("dbr__mention");
//		sample.add("dbr__tweet");
//		sample.add("dbr__friends");
//		result.put("dbr__mention_JOIN_dbr__tweet_JOIN_dbr__friends", sample);
		return result;
	}

	private Set<String> cleanup(Collection<String> res) {

		Set<String> result = new HashSet<>();
		for (String elem : res) {
			result.add(elem.replace("_TRPO", "").replace("_TRPS", ""));
		}
		return result;
	}

	private String cleanup(String input) {
		return input.replace("_TRPS", "").replace("_TRPO", "");
	}

	private Set<String> costModel(HashMap<Set<String>, Long> patterns, int totalNumProperties) {

		//Return the one with the highest support
		double bestCost = 0;
		long support;
		Set<String> bestCandidate = null;
		Set<String> pattern;

		for (Map.Entry<Set<String>, Long> entry : patterns.entrySet()) {
			pattern = entry.getKey();
			//Cost: Criteria #1
			support = entry.getValue();
			//Cost: Criteria #2
			int numProperties = pattern.size();

			double cost = (threshold) * (Math.log(support) / Math.log(this.metadata.getNumQueries())) + (1 - threshold) * (Math.log(numProperties) / Math.log(totalNumProperties));
			//LOG.debug("Cost: "+ cost + " "+pattern + " SUPPORT: "+support);
			if (cost > bestCost) {
				bestCandidate = pattern;
				bestCost = cost;
			}
		}

//		double score;
//		double bestScore = 0;
//		ArrayList<String> tempList;
//		ArrayList<String> bestList = new ArrayList<>();
//
//		if(properties.size() > 2) {
//			//Criteria #1 : Check for average semantic relatedness
//			for (int i = properties.size() - 1; i > 0; i--) {
//				tempList = (ArrayList<String>) properties.subList(0, i);
//				score = computeAvgSemSimilarity(tempList);
//				if (score > bestScore) {
//					bestScore = score;
//					bestList = tempList;
//				}
//			}
//			return bestList;
//		}else{
//			return properties;
//		}
		//LOG.debug("BEST: "+ pattern + " SUPPORT: "+support);
		return bestCandidate;
	}

	private HashMap<Set<String>, Long> runFPGrowth(List<String> input, double minSupport) {

		HashMap<Set<String>, Long> results = new HashMap<>();
		JavaRDD<String> data = sc.parallelize(input);
		JavaRDD<List<String>> transactions = data.map(line -> {
			String[] parts = line.split(" ");
			return Arrays.asList(parts);
		});

		FPGrowth fpg = new FPGrowth()
				.setMinSupport(minSupport);
		FPGrowthModel<String> model = fpg.run(transactions);

		for (FPGrowth.FreqItemset<String> itemset : model.freqItemsets().toJavaRDD().collect()) {
			//System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
			if (itemset.javaItems().size() > 1)
				results.put(new HashSet(itemset.javaItems()), itemset.freq());
		}

//		LOG.debug(">>> FPGrowth Patterns ");
//		results.entrySet().forEach(LOG::debug);

		return results;
	}

	public void learn() {
		LOG.info("Training Data Size : " + this.metadata.getUniquePropertiesPatterns().size());
		Collection<String> sentences = this.metadata.getUniquePropertiesPatterns();
		SentenceIterator iter;
//		try {
		//iter = new BasicLineIterator(this.metadata.getLocalDbPath() + "training.txt");
		iter = new CollectionSentenceIterator(sentences);
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		}

		// Split on white spaces in the line to get words
		TokenizerFactory t = new DefaultTokenizerFactory();
		t.setTokenPreProcessor(new CommonPreprocessor());

		LOG.info("Building model....");
		this.vec = new Word2Vec.Builder()
				.minWordFrequency(1)
				.windowSize(1)
				.iterate(iter)
				.tokenizerFactory(t)
				.build();

		this.vec.fit();

		//LOG.info("Fitting Word2Vec model....");
		//this.vec.fit();
		LOG.info("Saving the latest model");
		//WordVectorSerializer.writeWord2Vec(this.vec, metadata.getLocalDbPath() + "word2vec-model.txt");
	}

	private Map<String, Double> sortPropertiesBySimilarity(String headProperty, ArrayList<String> sortedProperties) {

		double score;
		Map<String, Double> candidates = new HashMap<>();
		for (int i = 0; i < sortedProperties.size(); i++) {
			if (this.vec.getVocab().containsWord(sortedProperties.get(i)) && this.vec.getVocab().containsWord(headProperty)) {
				score = this.vec.similarity(headProperty, sortedProperties.get(i));
				LOG.debug(headProperty + " --> " + sortedProperties.get(i) + " = " + score);
				if (score > 0.9)
					candidates.put(sortedProperties.get(i), score);
			}
		}
		return MapUtils.sortByValue(candidates, "dec");
	}

	private Map<String, Integer> sortPropertiesByFrequency(ArrayList<String> properties) {
		//Get the map entries for properties in the list
		HashMap<String, Integer> entries = this.metadata.getPropertyFrequency();
		Map<String, Integer> candidates = new HashMap<>();
		for (String property : properties) {
			if (entries.containsKey(property)) {
				candidates.put(property, entries.get(property));
			}
		}
		return MapUtils.sortByValue(candidates, "dec");
	}

	private double computeAvgSemSimilarity(ArrayList<String> tempList) {
		double avgScore = 0;
		//Calculate the centroid
		double[] avgVec = new double[vec.getWordVector(tempList.get(0)).length];
		for (String itm : tempList) {
			double[] v = vec.getWordVector(itm);
			for (int i = 0; i < v.length; i++) {
				avgVec[i] += v[i];
			}
		}
		for (int i = 0; i < avgVec.length; i++) {
			avgVec[i] /= avgVec.length;
		}
		//Calculate SMSE
		for (String itm : tempList) {
			double[] v = vec.getWordVector(itm);
			for (int i = 0; i < v.length; i++) {
				avgVec[i] += v[i];
			}
		}
		return avgScore;
	}

	public HashMap<String, Set<String>> runCooccurenceApproach() {

		HashMap<String, Set<String>> goldSet = new HashMap<>();
		ArrayList<String> joins = metadata.getUniqueQueryJoinPatterns();
		for (String join : joins) {
			List<String> parts = Arrays.asList(join.split(" "));
			Set<String> res = cleanup(parts);
			for (String q : res) {
				if (goldSet.containsKey(q))
					goldSet.get(cleanup(q)).addAll(res);
				else {
					HashSet<String> lst = new HashSet<>();
					lst.addAll(res);
					goldSet.put(cleanup(q), lst);
				}
			}
		}

//		for(Map.Entry<String,Set<String>> entry : goldSet.entrySet()){
//			Set<String> elems = entry.getValue();
//			Map<String,Integer> sorted = sortPropertiesByFrequency(new ArrayList<>(elems));
//			HashSet<String> sortedProps = new HashSet<>();
//			int threshold = 0;
//			for(Map.Entry<String,Integer> en:sorted.entrySet()){
//				if(threshold < 10) {
//					sortedProps.add(en.getKey());
//					threshold +=1;
//				}else {
//					break;
//				}
//			}
//			goldSet.put(entry.getKey(),sortedProps);
//		}
		return goldSet;
	}
}
