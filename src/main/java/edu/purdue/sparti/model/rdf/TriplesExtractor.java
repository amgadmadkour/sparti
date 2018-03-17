package edu.purdue.sparti.model.rdf;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.util.URIref;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Amgad Madkour
 */
public class TriplesExtractor {

	private RDFDataset RDFDatasetName;
	private ArrayList<String> properties;
	private HashMap<String, ArrayList<String>> joinEntries;
	private HashMap<String, String> bgpReplacement;

	public TriplesExtractor(RDFDataset RDFDatasetName) {
		this.joinEntries = new HashMap<>();
		this.properties = new ArrayList<>();
		this.RDFDatasetName = RDFDatasetName;
		Logger.getLogger("org.apache.jena.arq.info").setLevel(Level.OFF);
		Logger.getLogger("org.apache.jena.arq.exec").setLevel(Level.OFF);
	}

	public void extractByID(String inputQuery) {

		Query query;

		try {
			query = QueryFactory.create(inputQuery);
		} catch (Exception exp) {
			exp.printStackTrace();
			return;
		}

		if (query.isSelectType()) {
			Op opRoot = Algebra.compile(query);
			BGPVisitorByID visitor = new BGPVisitorByID(RDFDatasetName);
			NameSpaceHandler nsHandler = new NameSpaceHandler(RDFDatasetName);

			OpWalker.walk(opRoot, visitor);
			this.bgpReplacement = visitor.getBgpReplacements();

			//Get visited properties
			for (String property : visitor.getProperties()) {
				this.properties.add(nsHandler.parse(property));
			}

			//Get visited variable entries
			for (String variable : visitor.getJoinVariableEntries().keySet()) {
				ArrayList<Triple> list = visitor.getJoinVariableEntries().get(variable);
				if (list.size() > 1) {
					ArrayList<String> joins = new ArrayList<>();
					for (Triple t : list) {
						Node sub = t.getSubject();
						Node property = t.getPredicate();
						Node obj = t.getObject();
						if (property.isURI()) {
							String prefixed = nsHandler.parse(URIref.decode(property.toString()));
							if (sub.isVariable() && sub.getName().equals(variable))
								joins.add(prefixed + "_TRPS"); //Triple Subject
							if (obj.isVariable() && obj.getName().equals(variable))
								joins.add(prefixed + "_TRPO"); //Triple Object
						}
					}
					joinEntries.put(variable, joins);
				}
			}
		}
	}

	public void extract(String inputQuery) {

		Query query;

		try {
			query = QueryFactory.create(inputQuery);
		} catch (Exception exp) {
			exp.printStackTrace();
			return;
		}

		if (query.isSelectType()) {
			Op opRoot = Algebra.compile(query);
			BGPVisitor visitor = new BGPVisitor(RDFDatasetName);
			NameSpaceHandler nsHandler = new NameSpaceHandler(RDFDatasetName);

			OpWalker.walk(opRoot, visitor);

			//Get visited properties
			for (String property : visitor.getProperties()) {
				this.properties.add(nsHandler.parse(property));
			}

			//Get visited variable entries
			for (String variable : visitor.getJoinVariableEntries().keySet()) {
				ArrayList<Triple> list = visitor.getJoinVariableEntries().get(variable);
				if (list.size() > 1) {
					ArrayList<String> joins = new ArrayList<>();
					for (Triple t : list) {
						Node sub = t.getSubject();
						Node property = t.getPredicate();
						Node obj = t.getObject();
						if (property.isURI()) {
							String prefixed = nsHandler.parse(URIref.decode(property.toString()));
							if (sub.isVariable() && sub.getName().equals(variable))
								joins.add(prefixed + "_TRPS"); //Triple Subject
							if (obj.isVariable() && obj.getName().equals(variable))
								joins.add(prefixed + "_TRPO"); //Triple Object
						}
					}
					joinEntries.put(variable, joins);
				}
			}
		}
	}


	public ArrayList<String> getProperties() {
		return this.properties;
	}

	public HashMap<String, ArrayList<String>> getJoinEntries() {
		return joinEntries;
	}

	public HashMap<String, String> getBgpReplacement() {
		return this.bgpReplacement;
	}
}