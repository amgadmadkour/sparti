package edu.purdue.sparti.partitioning;

import edu.purdue.sparti.metadata.SpartiMetadata;
import edu.purdue.sparti.partitioning.spark.SparkSemanticPartitioning;
import edu.purdue.sparti.utils.CliParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Amgad Madkour
 */
public class SpartiPartitioner {

	private static Logger LOG = Logger.getLogger(SpartiPartitioner.class.getName());

	public static void main(String[] args) {

		Logger.getLogger(LOG.getName()).setLevel(Level.DEBUG);

		SpartiMetadata metadata = new SpartiMetadata();
		new CliParser(args).parsePartitionParams(metadata);
		metadata.loadMetaData();
		metadata.loadFilters();
		LOG.info("Loading completed, attempting to discover partitions");

		SparkSemanticPartitioning sparti = new SparkSemanticPartitioning(metadata);
		sparti.parseQueryDataset();
		List<Map> vpTablesList = metadata.getVpTablesList();
		ArrayList<String> properties = new ArrayList<>();
		for (Map vpTable : vpTablesList) {
			properties.add(vpTable.get("tableName").toString());
		}

		HashMap<String, Set<String>> partitions = null;

		if (metadata.getPrecomputationAlgorithm() == SpartiAlgorithm.COOCCURRENCE) {
			LOG.debug(">>>>>>>>>>>>>>>>Using CO-OCCURRANCE<<<<<<<<<<<<<<<<<<<<<<");
			partitions = sparti.runCooccurenceApproach();
		} else if (metadata.getPrecomputationAlgorithm() == SpartiAlgorithm.FPGROWTH) {
			LOG.debug(">>>>>>>>>>>>>>>>Using FPG-Growth<<<<<<<<<<<<<<<<<<<<<<<<<");
			sparti.setThreshold(0.1);
			partitions = sparti.runFPGrowthApproach(properties);
		}

		partitions.entrySet().forEach(LOG::debug);

		if (partitions.size() > 0) {
			sparti.createPartitions(partitions);
			LOG.info("Created SPARTI tables");
		}

		LOG.info("Finished probing partitions");
	}
}
