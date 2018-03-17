package edu.purdue.sparti.queryprocessing.storage;

import edu.purdue.sparti.metadata.SpartiMetadata;
import edu.purdue.sparti.queryprocessing.storage.spark.SparkStorage;
import edu.purdue.sparti.utils.CliParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * @author Amgad Madkour
 */
public class SpartiStoreCreator {

	private static final Logger LOG = Logger.getLogger(SpartiStoreCreator.class.getName());

	public static void main(String[] args) {

        Logger.getLogger(LOG.getName()).setLevel(Level.INFO);

		LOG.info("Starting Store Creator");

		//Parse the input and aggregate the meta-data
		SpartiMetadata metadata = new SpartiMetadata();
		new CliParser(args).parseStorageParams(metadata);

		LOG.debug(metadata);

		if (metadata.getStoreType() == ComputationType.SPARK) {
			LOG.info("Using Spark to store the data");

			SparkStorage store = new SparkStorage(metadata);
			boolean status = store.createStore(
					metadata.getDatasetPath(),
					metadata.getDatasetSeparator(),
					metadata.getLocalDbPath(),
					metadata.getHdfsDbPath(),
					metadata.getBenchmarkName()
			);
			if (status == true)
				LOG.info("Success");
			else
				LOG.error("Failed to create store");
		}
	}
}
