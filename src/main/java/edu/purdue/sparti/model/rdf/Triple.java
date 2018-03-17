package edu.purdue.sparti.model.rdf;


import java.io.Serializable;

public class Triple implements Serializable {

	private String sub;
	private String prop;
	private String obj;

	public Triple() {
	}

	public Triple(String sub, String prop, String obj) {
		this.sub = sub;
		this.prop = prop;
		this.obj = obj;
	}

	public String getSub() {
		return sub;
	}

	public void setSub(String sub) {
		this.sub = sub;
	}

	public String getProp() {
		return prop;
	}

	public void setProp(String prop) {
		this.prop = prop;
	}

	public String getObj() {
		return obj;
	}

	public void setObj(String obj) {
		this.obj = obj;
	}

	public String toString() {
		return sub + " " + prop + " " + obj;
	}
}
