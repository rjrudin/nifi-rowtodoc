package com.marklogic.nifi.processors.rowtodoc;

import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@CapabilityDescription("Executes a SQL query and returns each batch of rows as a list of column maps - i.e. a List<Map<String, Object>>")
@TriggerSerially
public class ExecuteSQLToColumnMaps extends AbstractProcessor {

	protected List<PropertyDescriptor> properties;
	protected Set<Relationship> relationships;

	public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
		.name("Database Connection Pooling Service")
		.description("The Controller Service that is used to obtain connection to database")
		.required(true)
		.identifiesControllerService(DBCPService.class)
		.build();

	public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
		.name("SQL query")
		.required(true)
		.description("The main SQL query to run")
		.addValidator(StandardValidators.NON_BLANK_VALIDATOR)
		.build();

	public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
		.name("Batch size")
		.description("Number of rows to convert into column maps before passing to the next processor")
		.required(true)
		.defaultValue("100")
		.addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
		.build();

	protected static final Relationship SUCCESS = new Relationship.Builder()
		.name("SUCCESS")
		.description("Success relationship")
		.build();

	@Override
	public void init(final ProcessorInitializationContext context) {
		List<PropertyDescriptor> list = new ArrayList<>();
		list.add(DBCP_SERVICE);
		list.add(QUERY);
		list.add(BATCH_SIZE);
		properties = Collections.unmodifiableList(list);

		Set<Relationship> set = new HashSet<>();
		set.add(SUCCESS);
		relationships = Collections.unmodifiableSet(set);
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
		final String query = context.getProperty(QUERY).getValue();
		final int batchSize = context.getProperty(BATCH_SIZE).asInteger();

		final ColumnMapRowMapper rowMapper = new ColumnMapRowMapper();

		Connection connection = dbcpService.getConnection();
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try {
			getLogger().info("Executing query: " + query);
			preparedStatement = connection.prepareStatement(query);
			resultSet = preparedStatement.executeQuery();
			int rowNumber = 0;
			List<Map<String, Object>> columnMaps = new ArrayList<>();
			while (resultSet.next()) {
				columnMaps.add(rowMapper.mapRow(resultSet, rowNumber));
				rowNumber++;
				if (rowNumber >= batchSize) {
					getLogger().info("Row number: " + rowNumber + "; sending batch of size: " + columnMaps.size());
					FlowFile flowFile = session.create();
					flowFile = session.write(flowFile, new ColumnMapsWriter(columnMaps));
					session.transfer(flowFile, SUCCESS);
					session.commit();
					rowNumber = 0;
					columnMaps = new ArrayList<>();
				}
			}

			// ResultSet is complete, so send one more FlowFile
			if (!columnMaps.isEmpty()) {
				getLogger().info("Sending final batch of size: " + columnMaps.size());
				FlowFile flowFile = session.create();
				flowFile = session.write(flowFile, new ColumnMapsWriter(columnMaps));
				session.transfer(flowFile, SUCCESS);
			}
		} catch (SQLException ex) {
			throw new ProcessException(ex);
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					// ignore
				}
			}
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					// ignore
				}
			}
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

