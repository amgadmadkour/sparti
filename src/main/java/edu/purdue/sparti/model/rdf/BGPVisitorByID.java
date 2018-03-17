package edu.purdue.sparti.model.rdf;

import com.hp.hpl.jena.graph.*;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpTriple;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Amgad Madkour
 */
public class BGPVisitorByID extends OpVisitorBase {

	private ArrayList<String> properties;
	private HashMap<String, ArrayList<Triple>> joinVariableEntries;
	private HashMap<String, String> bgpReplacements;
	private static Logger LOG = Logger.getLogger(BGPVisitorByID.class.getName());
	private NameSpaceHandler nsHandler;
	int ID;

	public BGPVisitorByID(RDFDataset ns) {
		Logger.getLogger(LOG.getName()).setLevel(Level.DEBUG);
		this.properties = new ArrayList<>();
		this.joinVariableEntries = new HashMap<>();
		this.bgpReplacements = new HashMap<>();
		this.ID = 0;
		this.nsHandler = new NameSpaceHandler(ns);
	}

	@Override
	public void visit(OpBGP opBGP) {
		BasicPattern p = opBGP.getPattern();
		for (Triple t : p) {
			this.ID += 1;

			String sub = nsHandler.parse(t.getSubject().toString());
			String pred = nsHandler.parse(t.getPredicate().toString());
			String predID = pred + "_" + ID;
			String obj = nsHandler.parse(t.getObject().toString());
			Triple t1 = new Triple(t.getSubject(), NodeFactory.createURI(predID), t.getObject());

			if (sub.contains("http")) {
				sub = "<" + sub + ">";
			}
			if (obj.contains("http")) {
				obj = "<" + obj + ">";
			}

			String original = sub.replace("__", ":") + " " + pred.replace("__", ":") + " " + obj.replace("__", ":");
			String replacement = sub.replace("__", ":") + " " + pred.replace("__", ":") + "_" + ID + " " + obj.replace("__", ":");
			bgpReplacements.put(original, replacement);

			Node subject = t1.getSubject();
			if (subject.isVariable()) {
				if (this.joinVariableEntries.containsKey(subject.getName())) {
					this.joinVariableEntries.get(subject.getName()).add(t1);
				} else {
					ArrayList<Triple> newList = new ArrayList<>();
					newList.add(t1);
					this.joinVariableEntries.put(subject.getName(), newList);
				}
			}

			Node prop = t1.getPredicate();
			if (prop.isURI()) {
				this.properties.add(predID);
			}

			Node object = t1.getObject();
			if (object.isVariable()) {
				if (this.joinVariableEntries.containsKey(object.getName())) {
					this.joinVariableEntries.get(object.getName()).add(t1);
				} else {
					ArrayList<Triple> newList = new ArrayList<>();
					newList.add(t1);
					this.joinVariableEntries.put(object.getName(), newList);
				}
			}
		}
	}

	@Override
	public void visit(OpTriple opTriple) {
	}

	public HashMap<String, ArrayList<Triple>> getJoinVariableEntries() {
		return joinVariableEntries;
	}

	public ArrayList<String> getProperties() {
		return this.properties;
	}

	public HashMap<String, String> getBgpReplacements() {
		return this.bgpReplacements;
	}

}