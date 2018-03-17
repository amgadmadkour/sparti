package edu.purdue.sparti.metadata;

import java.util.List;

/**
 * @author Amgad Madkour
 */
public class Table {
	private String tableName;
	private List<String> columns;
	private int size;
	private float ratio;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<String> getColumns() {
		return columns;
	}

	public void setColumns(List<String> columns) {
		this.columns = columns;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public float getRatio() {
		return ratio;
	}

	public void setRatio(float ratio) {
		this.ratio = ratio;
	}
}
