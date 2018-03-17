package edu.purdue.sparti.queryprocessing.storage;

import edu.purdue.sparti.model.rdf.RDFDataset;

/**
 * @author Amgad Madkour
 */
public interface SpartiStorage {

	boolean createStore(String datasetPath,
	                    String separator,
	                    String localdbPath,
	                    String hdfsdbPath,
	                    RDFDataset RDFDatasetName);
}
