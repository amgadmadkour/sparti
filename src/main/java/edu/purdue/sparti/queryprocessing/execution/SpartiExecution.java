package edu.purdue.sparti.queryprocessing.execution;

import edu.purdue.sparti.model.rdf.RDFDataset;

/**
 * @author Amgad Madkour
 */
public interface SpartiExecution {
	boolean execute(String queryPath, String localdbPath, String hdfsdbPath, RDFDataset RDFDatasetName);
}
