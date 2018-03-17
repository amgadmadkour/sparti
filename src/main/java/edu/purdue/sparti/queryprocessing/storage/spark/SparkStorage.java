package edu.purdue.sparti.queryprocessing.storage.spark;

import com.google.common.hash.HashFunction;
import com.google.gson.JsonElement;
import edu.purdue.sparti.metadata.SpartiMetadata;
import edu.purdue.sparti.model.rdf.NameSpaceHandler;
import edu.purdue.sparti.model.rdf.RDFDataset;
import edu.purdue.sparti.model.rdf.Triple;
import edu.purdue.sparti.queryprocessing.storage.SpartiStorage;
import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.json.BloomFilterConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;

/**
 * @author Amgad Madkour
 */

public class SparkStorage implements SpartiStorage, Serializable {

	private static final Logger LOG = Logger.getLogger(SparkStorage.class.getName());
	private static JavaSparkContext sc;
	private static SQLContext sqlContext;
	private SpartiMetadata metadata;

	public SparkStorage(SpartiMetadata metadata) {
		Logger.getLogger(LOG.getName()).setLevel(Level.INFO);
		this.metadata = metadata;
		SparkConf conf = new SparkConf()
				.setAppName("SpartiStorage")
				.set("spark.sql.inMemoryColumnarStorage.compressed", "true")
				.set("spark.sql.inMemoryColumnarStorage.batchSize", "20000")
				.set("spark.network.timeout", "30000")
				.set("spark.sql.parquet.filterPushdown", "true")
				.set("spark.driver.maxResultSize", "20g")
				.set("spark.sql.parquet.cacheMetadata", "true")
				.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC");

		sc = new JavaSparkContext(conf);
		sqlContext = new SQLContext(sc);
	}

	public boolean createStore(String datasetPath, String separator, String localdbPath, String hdfsdbPath, RDFDataset benchmarkName) {

		NameSpaceHandler nsHandler = new NameSpaceHandler(benchmarkName);
		Map<String, Object> databaseInfo;
		List<Map> vpTablesInfo;

		DataFrame triples = loadTriples(datasetPath, separator, nsHandler);
		triples.cache();
		LOG.info("Reading and caching the data completed");
		triples.toDF().write().mode(SaveMode.Overwrite).parquet(hdfsdbPath + "base.parquet");
		List<Row> uniqueProperties = triples.select(col("prop")).distinct().collectAsList();
		long numProperties = uniqueProperties.size();
		long numTriples = triples.count();
		LOG.info("RDFDataset Size :" + triples.count());
		LOG.info("Number of properties : " + numProperties);

		databaseInfo = metadata.getDbInfo();
		//Metadata information about the data currently in the database
		databaseInfo.put("numVPTables", numProperties);
		databaseInfo.put("numProperties", numProperties);
		databaseInfo.put("numSpartiTables", 0);
		databaseInfo.put("numTriples", numTriples);
		databaseInfo.put("numProperties", numProperties);
		databaseInfo.put("numTuples", numTriples);

		vpTablesInfo = metadata.getVpTablesList();

		File f = new File(localdbPath + "filters");
		f.mkdirs();

		HashMap<String, Integer> propertyMap = new HashMap<>();

		int count = 0;
		for (Row r : uniqueProperties) {
			count += 1;
			LOG.info("Processing (" + count + "/" + numProperties + ") : " + r.getString(0));
			String property = r.getString(0);
			DataFrame result = createPropertyTable(triples, property);
			int resultSize = (int) result.count();

			//BloomFilter subJson = computeBloomFilter(result, "sub", resultSize);
			//BloomFilter objJson = computeBloomFilter(result, property, resultSize);

			HashMap<String, Object> vpTable = new HashMap<>();
			vpTable.put("tableName", property);
			vpTable.put("numTuples", resultSize);
			vpTable.put("ratio", (float) resultSize / (float) triples.count());
			vpTablesInfo.add(vpTable);
//			vpTable.put("sub", subJson.toString());
//			vpTable.put(property, objJson.toString());

			FileOutputStream fout = null;
			ObjectOutputStream oos = null;

			try {
				BloomFilter subJson = new FilterBuilder(resultSize, 0.001).buildBloomFilter();
				List<Row> subs = result.select(col("sub")).distinct().collectAsList();
				for (Row sub : subs) {
					subJson.add(sub.getString(0));
				}
				fout = new FileOutputStream(metadata.getLocalDbPath() + "filters/" + "sub_" + property);
				oos = new ObjectOutputStream(fout);
				JsonElement json = BloomFilterConverter.toJson(subJson);
				oos.writeObject(json.toString());
				oos.close();

				BloomFilter objJson = new FilterBuilder(resultSize, 0.001).buildBloomFilter();
				List<Row> objs = result.select(col("obj")).distinct().collectAsList();
				for (Row obj : objs) {
					objJson.add(obj.getString(0));
				}
				fout = new FileOutputStream(metadata.getLocalDbPath() + "filters/" + "obj_" + property);
				oos = new ObjectOutputStream(fout);
				JsonElement json2 = BloomFilterConverter.toJson(objJson);
				oos.writeObject(json2.toString());
				oos.close();

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			LOG.debug("Saved filters successfully for " + property);
			result.write().mode(SaveMode.Overwrite).parquet(hdfsdbPath + property);
			result.unpersist();
		}
		metadata.syncMetadata();
		return true;
	}

	//Optional: In case that not enough resources are available on the driver side
	private BloomFilter computeBloomFilter(DataFrame result, String columnName, int size) {
		//Create the bloom filter
		BloomFilter bloomFilter = result.select(col(columnName)).toJavaRDD().mapPartitions(r -> {
			BloomFilter bf = new FilterBuilder(size, 0.01).buildBloomFilter();
			while (r.hasNext()) {
				bf.add(r.next().getString(0));
			}
			ArrayList<BloomFilter> arr = new ArrayList<>();
			arr.add(bf);
			return arr;
		}).treeReduce((f1, f2) -> {
			f1.union(f2);
			f2.clear();
			return f1;
		}, 2);
		return bloomFilter;
	}

	public DataFrame createPropertyTable(DataFrame triples, String property) {
		List<StructField> fields = new ArrayList<>();
		StructField field1 = DataTypes.createStructField("sub", DataTypes.StringType, true);
		fields.add(field1);
		StructField field2 = DataTypes.createStructField("obj", DataTypes.StringType, true);
		fields.add(field2);
		StructType schema = DataTypes.createStructType(fields);
		DataFrame propertyTable = triples.select(col("sub"), col("prop"), col("obj"))
				.where(col("prop").equalTo(property)).select(col("sub"), col("obj"));

		return sqlContext.createDataFrame(propertyTable.toJavaRDD(), schema);
	}

	//A More portable data structure - EXPERIMENTAL !
	public DataFrame createCompactPropertyTable(Dataset triples, String property) {
		List<StructField> fields = new ArrayList<>();
		StructField field1 = DataTypes.createStructField("sub", DataTypes.StringType, true);
		fields.add(field1);
		StructField field2 = DataTypes.createStructField(property, DataTypes.createArrayType(DataTypes.StringType), true);
		fields.add(field2);
		//Group objects for every subject into a list and represent them in one row
		StructType schema = DataTypes.createStructType(fields);
		//Nested Column support
		DataFrame propertyTable = triples.select(col("subject"), col("property"), col("object"))
				.where(col("property").equalTo(property))
				.groupBy(col("subject"))
				.agg(collect_list(col("object"))).alias(property);

		return sqlContext.createDataFrame(propertyTable.toJavaRDD(), schema);
	}

	public DataFrame loadTriples(String datasetPath, String separator, NameSpaceHandler nsHandler) {
		JavaRDD<Triple> triples = sc.textFile(datasetPath)
				.map(value -> {
					if (value.endsWith("."))
						value = value.substring(0, value.length() - 2);
					String[] parts = value.split(separator);
					String sub = nsHandler.parse(parts[0]);
					String prop = nsHandler.parse(parts[1]);
					String obj = parts[2];

					if (obj.startsWith("<") && obj.endsWith(">"))
						obj = nsHandler.parse(parts[2]);
					return new Triple(sub, prop, obj);
				});

		return sqlContext.createDataFrame(triples, Triple.class);
	}
}
