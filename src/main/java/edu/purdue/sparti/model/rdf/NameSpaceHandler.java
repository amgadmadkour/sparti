package edu.purdue.sparti.model.rdf;

import java.io.*;
import java.util.HashMap;

/**
 * @author Amgad Madkour
 */
public class NameSpaceHandler implements Serializable {

	private HashMap<String, String> prefixList;

	public NameSpaceHandler(RDFDataset ns) {
		prefixList = new HashMap<>();
		loadNameSpace(ns);
	}

	private void loadNameSpace(RDFDataset ns) {
		try {
			//ClassLoader classLoader = getClass().getClassLoader();
			//File file = new File(classLoader.getResource("prefixes/dbpedia.txt").getFile());
			//InputStream jarStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("prefixes/dbpedia.txt");
			InputStream jarStream = null;
			if (ns == RDFDataset.DBPEDIA) {
				jarStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("prefixes/dbpedia.txt");
			} else if (ns == RDFDataset.WATDIV) {
				jarStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("prefixes/watdiv.txt");
			} else if (ns == RDFDataset.YAGO) {
				jarStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("prefixes/yago.txt");
			} else if (ns == RDFDataset.LUBM) {
				jarStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("prefixes/lubm.txt");
			}
			if (ns == RDFDataset.GENERIC) {
				return;
			}
			BufferedReader rdr = new BufferedReader(new InputStreamReader(jarStream));
			String temp;
			while ((temp = rdr.readLine()) != null) {
				String[] parts = temp.split("\t");
				prefixList.put(parts[1], parts[0]);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private String identifyPrefix(String uri) {
		for (String key : prefixList.keySet()) {
			if (uri.contains(key)) {
				return uri.replace(key, prefixList.get(key) + "__");
			}
		}
		return uri;
	}

	public String parse(String uri) {
		if (uri.startsWith("<") && uri.endsWith(">")) {
			uri = uri.substring(1, uri.length() - 1);
		} else if (uri.startsWith("\"") && uri.endsWith("\"")) {
			return uri;
		}
		return identifyPrefix(uri);
	}
}
