package com.marklogic.nifi.processors.rowtodoc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

@CapabilityDescription("Given a FlowFile containing a list of column maps, executes child queries to populate child data on each column map")
public class ExecuteChildQueriesOnColumnMaps extends AbstractColumnMapProcessor {

	protected List<PropertyDescriptor> properties;
	protected Set<Relationship> relationships;

	// TODO Pull this from a controller service?
	private ObjectMapper objectMapper = new ObjectMapper();

	private ChildQueryExecutor childQueryExecutor = new ChildQueryExecutor();

	public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
		.name("Database Connection Pooling Service")
		.description("The Controller Service that is used to obtain connection to database")
		.required(true)
		.identifiesControllerService(DBCPService.class)
		.build();

	public static final PropertyDescriptor CHILD_QUERY_JSON = new PropertyDescriptor.Builder()
		.name("Child query JSON")
		.required(true)
		.description("JSON specifying the child queries to run")
		.addValidator(StandardValidators.NON_BLANK_VALIDATOR)
		.build();

	protected static final Relationship SUCCESS = new Relationship.Builder()
		.name("SUCCESS")
		.description("Success relationship")
		.build();

	protected static final Relationship CONTENT = new Relationship.Builder()
		.name("CONTENT")
		.description("Each JSON document will be written to this relationship")
		.build();

	@Override
	public void init(final ProcessorInitializationContext context) {
		List<PropertyDescriptor> list = new ArrayList<>();
		list.add(DBCP_SERVICE);
		list.add(CHILD_QUERY_JSON);
		properties = Collections.unmodifiableList(list);

		Set<Relationship> set = new LinkedHashSet<>();
		set.add(SUCCESS);
		relationships = Collections.unmodifiableSet(set);
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile != null) {
			List<Map<String, Object>> columnMapList = deserializeColumnMapList(session, flowFile);
			getLogger().info("Number of column maps received: " + columnMapList.size());
			if (!columnMapList.isEmpty()) {
				executeChildQueries(context, columnMapList);
				flowFile = session.write(flowFile, new ColumnMapsWriter(columnMapList));
				session.transfer(flowFile, SUCCESS);
			}
		}
	}

	/**
	 * Execute the child queries defined by the child query JSON property against the given list of column maps.
	 *
	 * @param context
	 * @param columnMapList
	 */
	protected void executeChildQueries(ProcessContext context, List<Map<String, Object>> columnMapList) {
		final String childQueryJson = context.getProperty(CHILD_QUERY_JSON).getValue();
		TableQuery tableQuery;
		try {
			tableQuery = objectMapper.readerFor(TableQuery.class).readValue(childQueryJson);
		} catch (IOException e) {
			throw new ProcessException("Unable to read JSON for child queries: " + childQueryJson, e);
		}

		DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
		Connection connection = dbcpService.getConnection();
		try {
			childQueryExecutor.executeChildQueries(connection, tableQuery, columnMapList);
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					// ignore
				}
			}
		}
	}

	@Override
	public Set<Relationship> getRelationships() {
		return relationships;
	}

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return properties;
	}
}
