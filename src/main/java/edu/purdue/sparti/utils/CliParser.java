package edu.purdue.sparti.utils;

import edu.purdue.sparti.metadata.SpartiMetadata;
import edu.purdue.sparti.model.rdf.RDFDataset;
import edu.purdue.sparti.partitioning.SpartiAlgorithm;
import edu.purdue.sparti.partitioning.SpartiPartitioner;
import edu.purdue.sparti.queryprocessing.storage.ComputationType;
import org.apache.commons.cli.*;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author Amgad Madkour
 */
public class CliParser {

	private final Logger LOG = Logger.getLogger(CliParser.class.getName());
	private CommandLineParser parser;
	private CommandLine cmd;
	private Options options;
	private String[] args;

	public CliParser(String[] args) {
		this.parser = new BasicParser();
		this.args = args;
		this.options = new Options();

		BasicConfigurator.configure();
		Logger.getLogger(LOG.getName()).setLevel(Level.DEBUG);
	}

	private void help(String type) {
		HelpFormatter formater = new HelpFormatter();
		if (type.equals("store"))
			formater.printHelp("edu.purdue.sparti.queryprocessing.storage.SpartiStoreCreator", options);
		else if (type.equals("execute"))
			formater.printHelp("edu.purdue.sparti.queryprocessing.execution.SpartiExecutor", options);
		else if (type.equals("eval"))
			formater.printHelp("edu.purdue.sparti.evaluation.EvaluatePartitions", options);
		else if (type.equals("partition"))
			formater.printHelp("edu.purdue.sparti.partitioning.SpartiPartitioner", options);
		System.exit(0);
	}

	private void initStorageOptions() {
		options.addOption("l", "localdb", true, "Local database location (Contains Metadata)");
		options.addOption("r", "hdfsdb", true, "HDFS database location (Contains Parquet Files)");
		options.addOption("d", "dataset", true, "Create database for input dataset (NTriple Format Required");
		options.addOption("s", "separator", true, "Separator used between Subject/Property/Object (default is tab)");
		options.addOption("t", "computation", true, "Computation type (e.g. spark, mapreduce, centralized)");
		options.addOption("b", "benchmark", true, "Specify the benchmark/dataset name (e.g., WatDiv,LUBM or Generic)");
	}

	private void initExecutionOptions() {
		options.addOption("l", "localdb", true, "Local database location (Contains Metadata)");
		options.addOption("r", "hdfsdb", true, "HDFS database location (Contains Parquet Files)");
		options.addOption("q", "queryfile", true, "SPARQL Queries to execute");
		options.addOption("b", "benchmark", true, "Specify the benchmark/dataset name (e.g., WatDiv,LUBM or Generic)");
	}

	private void initEvalOptions() {
		options.addOption("l", "localdb", true, "Local database location (Contains Metadata)");
		options.addOption("q", "queryfile", true, "SPARQL Queries to execute");
		options.addOption("i", "inputdir", true, "Input Query Directory");
		options.addOption("b", "benchmark", true, "Specify the benchmark/dataset name (e.g., WatDiv,LUBM or Generic)");
	}


	private void initPartitionOptions() {
		options.addOption("l", "localdb", true, "Local database location (Contains Metadata)");
		options.addOption("r", "hdfsdb", true, "HDFS database location (Contains Parquet Files)");
		options.addOption("q", "queryfile", true, "SPARQL Queries to execute");
		options.addOption("a", "algorithm", true, "Precomputation Algorithm");
		options.addOption("b", "benchmark", true, "Specify the benchmark/dataset name (e.g., WatDiv,LUBM or Generic)");
	}

	/**
	 * Parse the command line arguments of the storage creator
	 *
	 * @param metadata Used for saving the argument values in the metadata object
	 */
	public void parseStorageParams(SpartiMetadata metadata) {

		initStorageOptions();

		try {

			cmd = parser.parse(options, args);
			if (cmd.hasOption("l") && cmd.hasOption("r") && cmd.hasOption("d") && cmd.hasOption("t") && cmd.hasOption("b")) {
				//Validate whether the local directory exists
				Path path = Paths.get(cmd.getOptionValue("l"));
				if (Files.notExists(path)) {
					Files.createDirectory(path);
				}
				String hdfsPath = cmd.getOptionValue("r");
				String datasetPath = cmd.getOptionValue("d");
				String localDbPath = cmd.getOptionValue("l");
				String separator;
				Separator sep = Separator.SPACE;
				ComputationType store = ComputationType.CENTRALIZED;
				RDFDataset benchmark;

				if (cmd.hasOption("s")) {
					separator = cmd.getOptionValue("s");
					if (separator.equals("tab")) {
						sep = Separator.TAB;
					} else {
						sep = Separator.SPACE;
					}
				}

				String st = cmd.getOptionValue("t");
				if (st.equalsIgnoreCase("spark")) {
					store = ComputationType.SPARK;
				}

				String bm = cmd.getOptionValue("b");
				if (bm.equalsIgnoreCase("watdiv")) {
					benchmark = RDFDataset.WATDIV;
				} else if (bm.equalsIgnoreCase("dbpedia")) {
					benchmark = RDFDataset.DBPEDIA;
				} else if (bm.equalsIgnoreCase("yago")) {
					benchmark = RDFDataset.YAGO;
				} else if (bm.equalsIgnoreCase("lubm")) {
					benchmark = RDFDataset.LUBM;
				} else {
					benchmark = RDFDataset.GENERIC;
				}


				//Set the metadata information so that we can use it as we go along
				metadata.setLocalDbPath(localDbPath);
				metadata.setHdfsDbPath(hdfsPath);
				metadata.setDatasetPath(datasetPath);
				metadata.setDatasetSeparator(sep);
				metadata.setBenchmarkName(benchmark);
				metadata.setStore(store);

			} else {
				help("store");
			}

		} catch (ParseException e) {
			help("store");
		} catch (IOException e) {
			LOG.log(Level.FATAL, e.getMessage());
			System.exit(1);
		}

	}

	public void parseExecutionParams(SpartiMetadata metadata) {

		initExecutionOptions();

		try {

			cmd = parser.parse(options, args);
			if (cmd.hasOption("l") && cmd.hasOption("r") && cmd.hasOption("q") && cmd.hasOption("b")) {
				//Validate whether the local directory exists
				Path path = Paths.get(cmd.getOptionValue("l"));
				if (Files.notExists(path)) {
					LOG.log(Level.FATAL, "Cannot find local database - EXITING");
					System.exit(1);
				}
				String hdfsPath = cmd.getOptionValue("r");
				String queryFilePath = cmd.getOptionValue("q");
				String localDbPath = cmd.getOptionValue("l");
				RDFDataset benchmark;

				String bm = cmd.getOptionValue("b");
				if (bm.equalsIgnoreCase("watdiv")) {
					benchmark = RDFDataset.WATDIV;
				} else if (bm.equalsIgnoreCase("dbpedia")) {
					benchmark = RDFDataset.DBPEDIA;
				} else if (bm.equalsIgnoreCase("yago")) {
					benchmark = RDFDataset.YAGO;
				} else if (bm.equalsIgnoreCase("lubm")) {
					benchmark = RDFDataset.LUBM;
				} else {
					benchmark = RDFDataset.GENERIC;
				}

				//Set the metadata information so that we can use it as we go along
				metadata.setLocalDbPath(localDbPath);
				metadata.setHdfsDbPath(hdfsPath);
				metadata.setQueryFilePath(queryFilePath);
				metadata.setBenchmarkName(benchmark);

			} else {
				help("execute");
			}

		} catch (ParseException e) {
			help("execute");
		}
	}

	public void parsePartitionParams(SpartiMetadata metadata) {
		initPartitionOptions();

		try {

			cmd = parser.parse(options, args);
			if (cmd.hasOption("l") && cmd.hasOption("r") && cmd.hasOption("q") && cmd.hasOption("b") && cmd.hasOption("a")) {
				//Validate whether the local directory exists
				Path path = Paths.get(cmd.getOptionValue("l"));
				if (Files.notExists(path)) {
					LOG.log(Level.FATAL, "Cannot find local database - EXITING");
					System.exit(1);
				}
				String hdfsPath = cmd.getOptionValue("r");
				String queryFilePath = cmd.getOptionValue("q");
				String localDbPath = cmd.getOptionValue("l");

				RDFDataset benchmark;
				SpartiAlgorithm algorithm;

				String bm = cmd.getOptionValue("b");
				if (bm.equalsIgnoreCase("watdiv")) {
					benchmark = RDFDataset.WATDIV;
				} else if (bm.equalsIgnoreCase("dbpedia")) {
					benchmark = RDFDataset.DBPEDIA;
				} else if (bm.equalsIgnoreCase("yago")) {
					benchmark = RDFDataset.YAGO;
				} else if (bm.equalsIgnoreCase("lubm")) {
					benchmark = RDFDataset.LUBM;
				} else {
					benchmark = RDFDataset.GENERIC;
				}

				String alg = cmd.getOptionValue("a");
				if (alg.equalsIgnoreCase("cor")) {
					algorithm = SpartiAlgorithm.COOCCURRENCE;
				} else {
					algorithm = SpartiAlgorithm.FPGROWTH;
				}

				//Set the metadata information so that we can use it as we go along
				metadata.setLocalDbPath(localDbPath);
				metadata.setHdfsDbPath(hdfsPath);
				metadata.setQueryFilePath(queryFilePath);
				metadata.setBenchmarkName(benchmark);
				metadata.setPrecomputationAlgorithm(algorithm);

			} else {
				help("partition");
			}

		} catch (ParseException e) {
			help("partition");
		}
	}

	public void parseEvalParams(SpartiMetadata metadata) {

		initEvalOptions();

		try {

			cmd = parser.parse(options, args);
			if (cmd.hasOption("l") && cmd.hasOption("i") && cmd.hasOption("q") && cmd.hasOption("b")) {
				//Validate whether the local directory exists
				Path path = Paths.get(cmd.getOptionValue("l"));
				if (Files.notExists(path)) {
					LOG.log(Level.FATAL, "Cannot find local database - EXITING");
					System.exit(1);
				}
				String inputDirPath = cmd.getOptionValue("i");
				String queryFilePath = cmd.getOptionValue("q");
				String localDbPath = cmd.getOptionValue("l");
				RDFDataset benchmark;

				String bm = cmd.getOptionValue("b");
				if (bm.equalsIgnoreCase("watdiv")) {
					benchmark = RDFDataset.WATDIV;
				} else if (bm.equalsIgnoreCase("dbpedia")) {
					benchmark = RDFDataset.DBPEDIA;
				} else if (bm.equalsIgnoreCase("lubm")) {
					benchmark = RDFDataset.LUBM;
				} else if (bm.equalsIgnoreCase("yago")) {
					benchmark = RDFDataset.YAGO;
				} else {
					benchmark = RDFDataset.GENERIC;
				}

				//Set the metadata information so that we can use it as we go along
				metadata.setLocalDbPath(localDbPath);
				metadata.setInputDirPath(inputDirPath);
				metadata.setQueryFilePath(queryFilePath);
				metadata.setBenchmarkName(benchmark);

			} else {
				help("eval");
			}

		} catch (ParseException e) {
			help("eval");
		}


	}
}
