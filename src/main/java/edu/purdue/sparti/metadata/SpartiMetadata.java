package edu.purdue.sparti.metadata;


import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.sun.scenario.effect.Bloom;
import edu.purdue.sparti.model.rdf.RDFDataset;
import edu.purdue.sparti.partitioning.SpartiAlgorithm;
import edu.purdue.sparti.queryprocessing.storage.ComputationType;
import edu.purdue.sparti.utils.Separator;
import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.json.BloomFilterConverter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.*;

/**
 * @author Amgad Madkour
 */
public class SpartiMetadata implements Serializable {

	//LOGGER
	private static Logger LOG = Logger.getLogger(SpartiMetadata.class.getName());

	private String datasetPath;
	private String datasetSeparator;
	private ComputationType store;
	private RDFDataset benchmarkName;
	private String queryFilePath;
	private Map dbInfo;
	private List<Map> vpTablesList;
	private List<Map> spartiTablesList;
	private ArrayList<String> queryJoinPatterns;
	private ArrayList<String> queryPropertiesPattern;
	private HashMap<String, Integer> propertyFrequency;
	private HashMap<String, HashMap<String, BloomFilter>> bloomFilters;
	private long numQueries;
	private String inputDirPath;
	private SpartiAlgorithm precomputationAlgorithm;

	public SpartiMetadata() {
		this.dbInfo = new HashMap<>();
		this.vpTablesList = new ArrayList<>();
		this.spartiTablesList = new ArrayList<>();
		this.queryJoinPatterns = new ArrayList<>();
		this.queryPropertiesPattern = new ArrayList<>();
		this.propertyFrequency = new HashMap<>();
		this.bloomFilters = new HashMap<>();
		Logger.getLogger(LOG.getName()).setLevel(Level.ERROR);
	}

	public HashMap<String, HashMap<String, BloomFilter>> getBloomFilters() {
		return this.bloomFilters;
	}
	public String getHdfsDbPath() {
		return this.dbInfo.get("hdfsDbPath").toString();
	}

	public void setHdfsDbPath(String hdfsDbPath) {
		this.dbInfo.put("hdfsDbPath", hdfsDbPath);
	}

	public String getLocalDbPath() {
		return this.dbInfo.get("localDbPath").toString();
	}

	public void setLocalDbPath(String localDbPath) {
		this.dbInfo.put("localDbPath", localDbPath);
	}

	public void loadMetaData() {

		Yaml databaseInfo = new Yaml();
		Yaml vpTablesInfo = new Yaml();
		Yaml spartiTablesInfo = new Yaml();

		try {


			FileReader dbReader = new FileReader(getLocalDbPath() + "/dbinfo.yaml");
			this.dbInfo = (Map) databaseInfo.load(dbReader);
			LOG.debug(this.dbInfo);
			FileReader vpTablesRdr = new FileReader(getLocalDbPath() + "/vptables.yaml");
			Iterator<Object> docs1 = vpTablesInfo.loadAll(vpTablesRdr).iterator();
			while (docs1.hasNext()) {
				this.vpTablesList.add((Map) docs1.next());
			}
			FileReader spartiTablesRdr = new FileReader(getLocalDbPath() + "/spartitables.yaml");
			Iterator<Object> docs2 = spartiTablesInfo.loadAll(spartiTablesRdr).iterator();
			while (docs2.hasNext()) {
				this.spartiTablesList.add((Map) docs2.next());
			}

//			File f = new File(getLocalDbPath() + "training.txt");
//
//			if (f.exists()) {
//				BufferedReader trainingRdr = new BufferedReader(new FileReader(f));
//				String tmp;
//				while ((tmp = trainingRdr.readLine()) != null) {
//					this.queryPropertiesPattern.add(tmp);
//				}
//			}

		} catch (IOException exp) {
			exp.printStackTrace();
		}
	}

	public void loadFilters() {

		File folder = new File(getLocalDbPath() + "filters/");
		File[] listOfFiles = folder.listFiles();

		LOG.debug("Loading " + listOfFiles.length + " filters");

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				String name = listOfFiles[i].getName();
				String propertyName = name;
				boolean sub;
				if (name.startsWith("sub_")) {
					String[] parts = name.split("sub\\_");
					propertyName = parts[1];
					sub = true;
				} else {
					String[] parts = name.split("obj\\_");
					propertyName = parts[1];
					sub = false;
				}
				try {
					FileInputStream fin = new FileInputStream(new File(getLocalDbPath() + "filters/" + name));
					ObjectInputStream oos = new ObjectInputStream(fin);
					String json = (String) oos.readObject();
					JsonElement elem = new JsonParser().parse(json);
					BloomFilter filter = BloomFilterConverter.fromJson(elem);
					if (!sub && this.bloomFilters.containsKey(propertyName)) {
						this.bloomFilters.get(propertyName).put("obj", filter);
					} else if (sub && this.bloomFilters.containsKey(propertyName)) {
						this.bloomFilters.get(propertyName).put("sub", filter);
					} else if (!sub && !this.bloomFilters.containsKey(propertyName)) {
						HashMap<String, BloomFilter> entry = new HashMap<>();
						entry.put("obj", filter);
						this.bloomFilters.put(propertyName, entry);
					} else if (sub && !this.bloomFilters.containsKey(propertyName)) {
						HashMap<String, BloomFilter> entry = new HashMap<>();
						entry.put("sub", filter);
						this.bloomFilters.put(propertyName, entry);
					}
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public String getDatasetPath() {
		return this.datasetPath;
	}

	public void setDatasetPath(String datasetPath) {
		this.datasetPath = datasetPath;
	}

	public String getQueryFilePath() {
		return queryFilePath;
	}

	public void setQueryFilePath(String queryFilePath) {
		this.queryFilePath = queryFilePath;
	}

	public String getDatasetSeparator() {
		return this.datasetSeparator;
	}

	public void setDatasetSeparator(Separator datasetSeparator) {
		if (datasetSeparator == Separator.TAB)
			this.datasetSeparator = "\\t";
		else if (datasetSeparator == Separator.SPACE)
			this.datasetSeparator = " ";
	}

	public ComputationType getStoreType() {
		return store;
	}

	public void setStore(ComputationType store) {
		this.store = store;
	}

	public RDFDataset getBenchmarkName() {
		return this.benchmarkName;
	}

	public void setBenchmarkName(RDFDataset benchmarkName) {
		this.benchmarkName = benchmarkName;
	}

	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("\nMETADATA" + "\n");
		buffer.append("--------" + "\n");
		buffer.append("hdfsDbPath: " + getHdfsDbPath() + "\n");
		buffer.append("localDbPath: " + getLocalDbPath() + "\n");
		buffer.append("datasetPath: " + getDatasetPath() + "\n");
		buffer.append("queryFilePath: " + getQueryFilePath() + "\n");

		if (this.datasetSeparator == "\\t")
			buffer.append("datasetSeparator: TAB" + "\n");
		else
			buffer.append("datasetSeparator: SPACE" + "\n");

		buffer.append("store: " + this.store.name() + "\n");
		buffer.append("benchmarkName: " + getBenchmarkName().name() + "\n");

		return buffer.toString();
	}

	public Map getDbInfo() {
		return this.dbInfo;
	}

	public List<Map> getVpTablesList() {
		return this.vpTablesList;
	}

	public Map getVpTableInfo(String tableName) {
		for (Map map : this.vpTablesList) {
			if (map.get("tableName").equals(tableName))
				return map;
		}
		return null;
	}

	public List<Map> getSpartiTablesList() {
		return this.spartiTablesList;
	}

	public boolean hasSpartiTable(String tableName) {
		for (Map entry : spartiTablesList) {
			if (entry.get("tableName").equals(tableName))
				return true;
		}
		return false;
	}

	public String getTableName(String property) {
		List<Map> spartiList = getSpartiTablesList();
		for (Map entry : spartiList) {
			String tableName = entry.get("tableName").toString().split("SPARTI\\_")[1];
			if (tableName.equals(property)) {
				return entry.get("tableName").toString();
			}
		}
		return property;
	}

	public void syncMetadata() {
		//File paths
		try {

			FileWriter databaseInfoWriter = new FileWriter(getLocalDbPath() + "dbinfo.yaml");
			FileWriter vpTableInfoWriter = new FileWriter(getLocalDbPath() + "vptables.yaml");
			FileWriter spartiTableInfoWriter = new FileWriter(getLocalDbPath() + "spartitables.yaml");
			//PrintWriter trainingWriter = new PrintWriter(getLocalDbPath() + "training.txt");

			DumperOptions options = new DumperOptions();
			options.setSplitLines(false);
			options.setPrettyFlow(true);

			//YAML Objects
			Yaml yamlDBInfo = new Yaml(options);
			Yaml yamlVPTables = new Yaml(options);
			Yaml yamlSpartiTables = new Yaml(options);

			yamlDBInfo.dump(this.dbInfo, databaseInfoWriter);
			yamlVPTables.dumpAll(this.vpTablesList.iterator(), vpTableInfoWriter);
			yamlSpartiTables.dumpAll(this.spartiTablesList.iterator(), spartiTableInfoWriter);

//			for (String line : this.queryPropertiesPattern) {
//				trainingWriter.println(line);
//			}

			databaseInfoWriter.close();
			vpTableInfoWriter.close();
			spartiTableInfoWriter.close();
//			trainingWriter.close();

		} catch (IOException exp) {
			exp.printStackTrace();
		}
	}

	public void addQueryJoinPatterns(HashMap<String, ArrayList<String>> patterns) {
		for (ArrayList<String> pattern : patterns.values()) {
			Arrays.sort(pattern.toArray());
			this.queryJoinPatterns.add(StringUtils.join(pattern, " "));
			//Order joins alphabetically

			//String join = StringUtils.join(pattern, "_JOIN_");
			//if (this.queryJoinPatterns.containsKey(join)) {
//				this.queryJoinPatterns.put(join, this.queryJoinPatterns.get(join) + 1);
//			} else {
//				this.queryJoinPatterns.put(join, 1);
//			}
		}
	}

	public List<String> getQueryJoinPatterns() {
		return this.queryJoinPatterns;
	}

	public void addQueryPropertiesPattern(ArrayList<String> pattern) {
		//Add frequency data structure
		for (String property : pattern) {
			if (this.propertyFrequency.containsKey(property)) {
				int freq = this.propertyFrequency.get(property);
				freq += 1;
				this.propertyFrequency.put(property, freq);
			} else {
				this.propertyFrequency.put(property, 1);
			}
		}
		//Add training pattern
		this.queryPropertiesPattern.add(StringUtils.join(pattern, " "));
	}

	public HashMap<String, Integer> getPropertyFrequency() {
		return this.propertyFrequency;
	}

	public void addSpartiPartition(String newTableName, long tableSize, String[] schema, List<String> partitionProperties) {
		/** Update the metadata information **/
		long numVPTables = Long.parseLong(getDbInfo().get("numVPTables").toString());
		long numSpartiTables = Long.parseLong(getDbInfo().get("numSpartiTables").toString());
		long newTableSize = tableSize;

		numVPTables -= 1;
		getDbInfo().put("numVPTables", numVPTables);

		numSpartiTables += 1;
		getDbInfo().put("numSpartiTables", numSpartiTables);

		HashMap newEntry = new HashMap();
		//size,tableName,ratio
		newEntry.put("tableName", newTableName);
		newEntry.put("schema", StringUtils.join(schema, ","));
		newEntry.put("numTuples", newTableSize);
		double size = Long.parseLong(getDbInfo().get("numTriples").toString());
		newEntry.put("ratio", tableSize / size);
		getSpartiTablesList().add(newEntry);
		newEntry.put("partitionProperties", StringUtils.join(partitionProperties, ","));
	}

	public ArrayList<String> getQueryPropertiesPatterns() {
		return this.queryPropertiesPattern;
	}

	public ArrayList<String> getUniquePropertiesPatterns() {

//		ArrayList<String> h = new ArrayList<>();
//		h.add("human interface computer");
//		h.add("survey user computer system response time");
//		h.add("eps user interface system");
//		h.add("system human system eps");
//		h.add("user response time");
//		h.add("trees");
//		h.add("graph trees");
//		h.add("graph minors trees");
//		h.add("graph minors survey");
//		h.add("I like graph and stuff");
//		h.add("I like trees and stuff");
//		h.add("Sometimes I build a graph");
//		h.add("Sometimes I build trees");
//
//		return h;

		ArrayList<String> results = new ArrayList<>();
		for (String pattern : this.queryPropertiesPattern) {
			HashSet<String> unique = new HashSet<>();
			String[] parts = pattern.split(" ");
			if (parts.length > 1) {
				for (String p : parts) {
					unique.add(p);
				}
				results.add(StringUtils.join(unique, " "));
			}
		}
		return results;
	}

	public ArrayList<String> getUniqueQueryJoinPatterns() {
		ArrayList<String> results = new ArrayList<>();
		for (String pattern : this.queryJoinPatterns) {
			HashSet<String> unique = new HashSet<>();
			String[] parts = pattern.split(" ");
			if (parts.length > 1) {
				for (String p : parts) {
					unique.add(p);
				}
				results.add(StringUtils.join(unique, " "));
			}
		}
		return results;
	}

	public void incrementSemanticColumnCount(String newTable, String columnName, long increment) {
		for (Map entry : this.spartiTablesList) {
			if (entry.get("tableName").equals(newTable)) {
				if (entry.containsKey(columnName)) {
					long currentCount = (Long) entry.get(columnName);
					currentCount += increment;
					entry.put(columnName, currentCount);
				} else {
					entry.put(columnName, increment);
				}
			}
		}
	}

	public int getSemanticColumnCount(String tableName, String cand) {
		for (Map entry : this.spartiTablesList) {
			if (entry.get("tableName").equals(tableName)) {
				if (entry.containsKey(cand)) {
					return (int) entry.get(cand);
				} else {
					return -1;
				}
			}
		}
		return -1;
	}

	public void setNumQueries(long numQueries) {
		this.numQueries = numQueries;
	}

	public long getNumQueries() {
		return this.numQueries;
	}

	public boolean addSpartiColumn(String tableName, String columnName) {
		for (Map entry : this.spartiTablesList) {
			if (entry.get("tableName").equals(tableName)) {
				if (entry.containsKey("schema")) {
					String schema = (String) entry.get("schema");
					schema = schema + "," + columnName;
					entry.put("schema", schema);
					return true;
				}
			}
		}
		return false;
	}

	public void removeVP(String partition) {
		ArrayList<Map> remSet = new ArrayList<>();
		for (Map entry : this.vpTablesList) {
			if (entry.get("tableName").equals(partition)) {
				remSet.add(entry);
			}
		}
		for (Map entry : remSet) {
			this.vpTablesList.remove(entry);
		}
	}

	public BloomFilter getVPBloomFilter(String tableName, String columnName) {
		return this.bloomFilters.get(tableName).get(columnName);
	}

	public Map getSpartiTableInfo(String tableName) {
		for (Map map : this.spartiTablesList) {
			if (map.get("tableName").equals(tableName))
				return map;
		}
		return null;
	}

	public void setInputDirPath(String inputDirPath) {
		this.inputDirPath = inputDirPath;
	}

	public String getInputDirPath() {
		return this.inputDirPath;
	}

	public SpartiAlgorithm getPrecomputationAlgorithm() {
		return precomputationAlgorithm;
	}

	public void setPrecomputationAlgorithm(SpartiAlgorithm precomputationAlgorithm) {
		this.precomputationAlgorithm = precomputationAlgorithm;
	}
}
