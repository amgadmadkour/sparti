package edu.purdue.sparti.evaluation;

import edu.purdue.sparti.metadata.SpartiMetadata;
import edu.purdue.sparti.model.rdf.NameSpaceHandler;
import edu.purdue.sparti.model.rdf.TriplesExtractor;
import edu.purdue.sparti.partitioning.spark.SparkSemanticPartitioning;
import edu.purdue.sparti.queryprocessing.translation.SPARQLQueryModifier;
import edu.purdue.sparti.utils.CliParser;
import org.apache.avro.generic.GenericData;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;


public class EvaluatePartitions {

	private static Logger LOG = Logger.getLogger(EvaluatePartitions.class.getName());
	private static SpartiMetadata metadata;

	public static void main(String[] args) {

		Logger.getLogger(LOG.getName()).setLevel(Level.DEBUG);

		LOG.info("Partitioning Evaluation");

		metadata = new SpartiMetadata();
		new CliParser(args).parseEvalParams(metadata);
		metadata.loadMetaData();

		try {

			int numQueries = 0;
			File folder = new File(metadata.getInputDirPath());
			File[] listOfFiles = folder.listFiles();
			ArrayList<String> queries = new ArrayList<>();

			Arrays.sort(listOfFiles);

			for (int i = 0; i < listOfFiles.length; i++) {
				if (listOfFiles[i].isFile()) {
					queries.add(listOfFiles[i].getName());
				}
			}

			HashMap<String, HashSet<String>> goldSet = new HashMap<>();

			for (String qryName : queries) {
				String qry = new String(Files.readAllBytes(Paths.get(metadata.getInputDirPath() + qryName)));

				SPARQLQueryModifier modifier = new SPARQLQueryModifier(new NameSpaceHandler(metadata.getBenchmarkName()));
				qry = modifier.convertBasePrefix(qry);
				numQueries += 1;

				TriplesExtractor extractor = new TriplesExtractor(metadata.getBenchmarkName());
				extractor.extract(qry);
				HashMap<String, ArrayList<String>> queryJoins = extractor.getJoinEntries();
				for (Map.Entry<String, ArrayList<String>> entry : queryJoins.entrySet()) {
					ArrayList<String> entrylst = entry.getValue();
					Set<String> res = cleanup(entrylst);
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
			}

			//LOG.info(">> Extracted properties from "+ numQueries + " queries");

			//goldSet.entrySet().forEach(LOG::info);

			SparkSemanticPartitioning sparti = new SparkSemanticPartitioning(metadata);

			//Discover partitions from the VP list
			List<Map> vpTablesList = metadata.getVpTablesList();
			ArrayList<String> properties = new ArrayList<>();
			for (Map vpTable : vpTablesList) {
				properties.add(vpTable.get("tableName").toString());
			}

			//Run the discovery algorithm
			sparti.parseQueryDataset();
			//sparti.learn();

			for (float i = 0; i <= 0.1; i += 0.1) {
				//double i = 0.5;
				//sparti.setThreshold(i);
				//HashMap<String, Set<String>> partitions = sparti.runFPGrowthApproach(properties);
				HashMap<String, Set<String>> partitions = sparti.runCooccurenceApproach();
				LOG.debug("Number of partitions : " + partitions.size());

				String list = "";
				for (String prop : properties) {
					if (partitions.containsKey(prop))
						list += "spartidb-yago/" + prop + " ";
				}

				LOG.debug(list);
				//HashMap<String, Set<String>> partitions = sparti.runWordEmbeddingApproach(properties);
				//partitions.entrySet().forEach(LOG::debug);
//				for(Map.Entry<String,Set<String>> ent:partitions.entrySet()){
//					LOG.debug(ent.getValue().size()+ " " + ent);
//				}

				double count = 0;
				ArrayList<Double> fMeasures = new ArrayList<>();

				for (Map.Entry<String, Set<String>> entry : partitions.entrySet()) {
					String propertyName = entry.getKey();
					Set<String> predictedList = entry.getValue();
					Set<String> actualList = goldSet.get(propertyName);

					if (predictedList == null || actualList == null || predictedList.size() == 0 || actualList.size() == 0)
						continue;

					double tp = 0;

					double recall, precision, fmeasure;

					for (String s : actualList) {
						if (predictedList.contains(s))
							tp += 1;
					}

					recall = tp / actualList.size();
					precision = tp / predictedList.size();

					if ((recall + precision) == 0)
						fmeasure = 0;
					else
						fmeasure = 2 * ((precision * recall) / (precision + recall));
					//LOG.info(propertyName + " Pattern: "+predictedList);
					fMeasures.add(fmeasure);

					count += 1;
				}

				double result = 0;
				for (double val : fMeasures) {
					result += val;
				}
				double avgFMeasure = result / count;
				LOG.info("\t" + i + "\t" + avgFMeasure);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static Set<String> cleanup(List<String> res) {

		HashSet<String> result = new HashSet<>();
		for (String elem : res) {
			result.add(elem.replace("_TRPO", "").replace("_TRPS", ""));
		}
		return result;
	}

	private static String cleanup(String input) {
		return input.replace("_TRPS", "").replace("_TRPO", "");
	}

}
