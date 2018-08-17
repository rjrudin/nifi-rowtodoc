package com.marklogic.nifi.processors.rowtodoc;

import java.util.ArrayList;
import java.util.List;

public class TableQuery {

	private String query;
	// TODO Support compound keys
	private String primaryKeyColumnName;
	private String propertyName;

	// Optional - for child table
	private String foreignKeyColumnName;

	private List<TableQuery> childQueries = new ArrayList<>();

	public TableQuery() {
		// Needed for JSON deserialization
	}

	public TableQuery(String query, String primaryKeyColumnName, String foreignKeyColumnName, String propertyName) {
		this.query = query;
		this.primaryKeyColumnName = primaryKeyColumnName;
		this.foreignKeyColumnName = foreignKeyColumnName;
		this.propertyName = propertyName;
	}

	public void addChildQuery(TableQuery tableQuery) {
		this.childQueries.add(tableQuery);
	}

	public String getQuery() {
		return query;
	}

	public String getPrimaryKeyColumnName() {
		return primaryKeyColumnName;
	}

	public String getPropertyName() {
		return propertyName;
	}

	public String getForeignKeyColumnName() {
		return foreignKeyColumnName;
	}

	public List<TableQuery> getChildQueries() {
		return childQueries;
	}
}
