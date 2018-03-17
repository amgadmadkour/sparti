package edu.purdue.sparti.queryprocessing.execution;

import edu.purdue.sparti.metadata.SpartiMetadata;
import edu.purdue.sparti.queryprocessing.execution.spark.SparkExecution;
import edu.purdue.sparti.utils.CliParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * @author Amgad Madkour
 */
public class SpartiExecutor {

	private static Logger LOG = Logger.getLogger(SpartiExecutor.class.getName());
	private static SpartiMetadata metadata;

	public static void main(String[] args) {

		Logger.getLogger(LOG.getName()).setLevel(Level.ERROR);

		LOG.info("Starting Execution Engine");

		metadata = new SpartiMetadata();
		new CliParser(args).parseExecutionParams(metadata);
		metadata.loadMetaData();

		SparkExecution engine = new SparkExecution(metadata);
//		System.out.println("Starting Spark executor");
//		boolean status = engine.execute(metadata.getQueryFilePath(),
//				metadata.getLocalDbPath(),
//				metadata.getHdfsDbPath(),
//				metadata.getBenchmarkName());
		boolean status = engine.executeQueries(metadata.getQueryFilePath());

		if (status == true)
			LOG.info("Successfully executed");
		else
			LOG.fatal("Error executing queries");
	}
}
