package edu.purdue.sparti;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import edu.purdue.sparti.metadata.SpartiMetadata;
import edu.purdue.sparti.utils.MapUtils;
import edu.purdue.sparti.utils.Permute;
import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.json.BloomFilterConverter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.util.CollectionsUtils;
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.embeddings.wordvectors.WordVectors;
import org.deeplearning4j.models.glove.Glove;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.models.word2vec.wordstore.VocabCache;
import org.deeplearning4j.plot.BarnesHutTsne;
import org.deeplearning4j.plot.Tsne;
import org.deeplearning4j.ui.UiConnectionInfo;
import org.deeplearning4j.ui.UiServer;
import org.deeplearning4j.util.SerializationUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import static org.apache.spark.sql.functions.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SCRATCH JAVA CLASS FOR TESTING ONLY
 *
 * @author Amgad Madkour
 */
public class Scratch {

	private static JavaSparkContext sc;
	private static SQLContext sqlContext;

	public static void init() {
		SparkConf conf = new SparkConf()
				.setAppName("Scratch");
		sc = new JavaSparkContext(conf);
		sqlContext = new SQLContext(sc);
	}

	private Path inputPath;
	private FileStatus[] inputFileStatuses;

	public static void main(String[] args) throws Exception {

		init();

		Path x = new Path("hdfs://172.18.11.205:8020/user/amadkour/" + args[0]);

		Configuration conf = new Configuration();
		conf.addResource(new Path("/etc/hadoop/conf.cloudera.hdfs/core-site.xml"));
		int count = 0;
		FileSystem fs = FileSystem.get(conf);
		List<String> files = getAllFilePath(x, fs);

		for (String file : files) {
			if (file.endsWith(".parquet") && file.contains("__")) {
				System.out.println(file);
				count++;
			}
		}

		System.out.println(count);

//		    String str = "dbr__fname_JOIN_dbr__lname_JOIN_dbr_account";
//		    String[] qparts = str.split("\\_JOIN\\_");
//			Permute perm = new Permute();
//			List<List<String>> joinColumns = perm.permute(Arrays.asList(qparts), 0);
//		    System.out.println(joinColumns);

//        WordVectors vec = WordVectorSerializer.loadTxtVectors(new File("test/dboutput/word2vec-model.txt"));
//        System.out.println(vec.wordsNearest("rdf__type",10));
//		ArrayList<String> x = new ArrayList<>();
//		x.add("a");
//		x.add("b");
//		x.add("c");
//		x.add("d");
//		x.add("e");
//
//		ArrayList<String> y = new ArrayList<>();
//		y.add("a");
//		y.add("b");
//		y.add("c");
//		y.add("f");
//		y.add("d");
//		y.add("e");
//
//		System.out.println(ListUtils.longestCommonSubsequence(x,y));


//		//Sample input
//		ArrayList<String> queryJoins = new ArrayList<>();
//		queryJoins.add("dbr__mention_TRPO_JOIN_dbr__tweet_TRPS");
//		queryJoins.add("dbr__tweet_TRPS_JOIN_dbr__mention_TRPS");
//
//		SpartiMetadata metadata = new SpartiMetadata();
//		metadata.setLocalDbPath("test/dboutput/");
//		metadata.loadMetaData();
//
//		DataFrame table = sqlContext.read().parquet("hdfs://localhost:19000/user/amadkour/dbtest/dbr__tweet_JOIN_dbr__friends_JOIN_dbr__mention_JOIN_dbr__lives");
//		table.cache();
//		table.show();
//		table.printSchema();
//
//		Column[] cols = new Column[2];
//		cols[0] = count(col("dbr__mention_TRPO_JOIN_dbr__friends_TRPO"));
//		cols[1] = count(col("dbr__friends_TRPO_JOIN_dbr__mention_TRPO"));
//
//		Row r = table.select(cols).collect()[0];
//		System.out.println(r);
		//table.cache();

//		//Configuration conf = sc.hadoopConfiguration();
//		Configuration conf = new Configuration();
//		conf.addResource(new Path("C:\\Users\\amgad\\hpc\\hadoop\\etc\\hadoop\\core-site.xml"));
//		try {
//			for(String partition:partitions) {
//				FileSystem fs = FileSystem.get(conf);
//				fs.rename(new Path(metadata.getHdfsDbPath()+partition), new
//						Path(metadata.getHdfsDbPath() + "tmp/"+partition));
//			}
//		} catch (IOException e) {
//			e.printStackTrace();
//		}

//
//		for (int m = 0; m < queryJoins.size(); m++) {
//
//			ArrayList<String> freqJoins = new ArrayList<>();
//			String[] qparts = queryJoins.get(m).split("\\_JOIN\\_");
//			Permute perm = new Permute();
//			List<List<String>> joinColumns = perm.permute(Arrays.asList(qparts), 0);
//			for (List combination : joinColumns) {
//				freqJoins.add(StringUtils.join(combination, "_JOIN_"));
//			}
//
//			//Prepare the schema of the new dataframes
//			ArrayList<String> newColumns = new ArrayList<>();
//			List<String> cols = Arrays.asList(table.schema().fieldNames());
//
//			for (String columnName : freqJoins) {
//				//Make sure we add unique column names
//				if (!cols.contains(columnName)) {
//					newColumns.add(columnName);
//					table = table.withColumn(columnName, lit(null).cast(DataTypes.IntegerType));
//				}
//			}
//
//			table.show();
//			table.printSchema();
//
//
//			StructType newSchema = table.schema();
//			JavaRDD<Row> res = table.javaRDD().map(r -> {
//				String[] fieldNames = r.schema().fieldNames();
//				Object[] elems = new Object[fieldNames.length];
//				//Iterate over each field
//				for (int i = 0; i < fieldNames.length; i++) {
//					//For each field, if its one of the new columns, then compute its bloom join
//					boolean matchBloomField = false;
//					for (int l = 0; l < newColumns.size(); l++) {
//						if (newColumns.get(l).equals(fieldNames[i])) {
//							matchBloomField = true;
//							//Compute the Bloom join
//							String[] tableAndJoin = fieldNames[i].split("\\_JOIN\\_");
//							Map<String, String> mapTableAndJoin = new HashMap<>();
//							//Join based on size
//							ArrayList<String> tablesList = new ArrayList<>();
//							for (int j = 0; j < tableAndJoin.length; j++) {
//								String join = null;
//								String tableName = null;
//								if (tableAndJoin[j].contains("TRPS")) {
//									String[] parts = tableAndJoin[j].split("\\_TRPS");
//									tableName = parts[0];
//									join = "sub";
//								} else if (tableAndJoin[j].contains("TRPO")) {
//									String[] parts = tableAndJoin[j].split("\\_TRPO");
//									tableName = parts[0];
//									join = tableName;
//								}
//								mapTableAndJoin.put(tableName, join);
//								tablesList.add(tableName);
//							}
//
//							String joinColumn = mapTableAndJoin.get(tablesList.get(0));
//							boolean notPresent = false;
//							for (int k = 1; k < tablesList.size(); k++) {
//								String jsonString = (String) metadata.getVpTableInfo(tablesList.get(k)).get(mapTableAndJoin.get(tablesList.get(k)));
//								JsonParser jp2 = new JsonParser();
//								JsonElement elem = jp2.parse(jsonString);
//								BloomFilter<String> bloomFilter = BloomFilterConverter.fromJson(elem);
//								//System.out.println(joinColumn + ":" + r.getString(r.fieldIndex(joinColumn)) + " in " + mapTableAndJoin.get(tablesList.get(k)));
//								String val = r.getString(r.fieldIndex(joinColumn));
//								if (val != null && bloomFilter.contains(val)) {
//									continue;
//								}
//								notPresent = true;
//								break;
//							}
//							if (notPresent)
//								elems[i] = null;
//							else
//								elems[i] = 1;
//						}
//					}
//					if (!matchBloomField)
//						elems[i] = r.get(i);
//				}
//				return RowFactory.create(elems);
//			});
//			table = sqlContext.createDataFrame(res, newSchema);
//			table.show();
//		}

//		StructType schema = DataTypes.createStructType(fields);
//		DataFrame newDF = sqlContext.createDataFrame(newOne,schema);
//
//		DataFrame res = table.select("sub");
//
//		JavaRDD newOne = table.toJavaRDD().map(r -> RowFactory.create(r.getString(0),r.size(),r.size()));
//
//		newDF.show();


//		newDF.show();
//
//		DataFrame res1 = table.join(newDF,table.col("sub").equalTo(newDF.col("sub")),"leftsemi").select("*");
//		res1.show();


//		DataFrame df_mention = sqlContext.read().parquet("hdfs://localhost:19000/user/amadkour/dbtest/dbr__mention");
//		DataFrame df_tweet = sqlContext.read().parquet("hdfs://localhost:19000/user/amadkour/dbtest/dbr__tweet");
//		DataFrame df_lives = sqlContext.read().parquet("hdfs://localhost:19000/user/amadkour/dbtest/dbr__lives");
//
//		DataFrame[] frames = new DataFrame[3];
//
//		frames[0] = df_mention;
//		frames[1] = df_tweet;
//		frames[2] = df_lives;
//
//		for (int i = 1; i < frames.length; i++) {
//			frames[0] = frames[0].join(frames[i],
//					frames[0].col("sub").equalTo(frames[i].col("sub")), "outer")
//					.withColumn("sub_", coalesce(frames[0].col("sub"), frames[i].col("sub")))
//					.drop("sub")
//					.withColumnRenamed("sub_", "sub");
//		}
//
//		//frames[0].show();
//
//		frames[0].write().parquet("hdfs://localhost:19000/user/amadkour/dbtest/dbr__mention&dbr__tweet");
//
//		DataFrame merged = sqlContext.read().parquet("hdfs://localhost:19000/user/amadkour/dbtest/dbr__mention&dbr__tweet");
//		merged.show();

//		df_tweet.join(df_mention,
//				df_mention.col("sub").equalTo(df_tweet.col("sub")),"outer")
//				.withColumn("sub_", coalesce(df_tweet.col("sub"), df_mention.col("sub")))
//				.drop("sub")
//				.withColumnRenamed("sub_","sub");

	}

	public static List<String> getAllFilePath(Path filePath, FileSystem fs) throws IOException {
		List<String> fileList = new ArrayList<String>();
		FileStatus[] fileStatus = fs.listStatus(filePath);
		for (FileStatus fileStat : fileStatus) {
			if (fileStat.isDirectory()) {
				fileList.addAll(getAllFilePath(fileStat.getPath(), fs));
//			} else {
//				//fileList.add(fileStat.getPath().toString());
//
//			}
			} else {
				//fileList.add(fileStat.getPath().toString());
				String fileName = fileStat.getPath().toString();
				fileList.add(fileName.substring(fileName.lastIndexOf("/") + 1));
			}
		}
		return fileList;
	}

}