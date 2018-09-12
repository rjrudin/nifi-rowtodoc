package com.marklogic.nifi.processors.rowtodoc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.sql.Date;
import java.util.*;

@CapabilityDescription("Given a FlowFile containing a list of column maps, converts each column map into a JSON document that is sent to the CONTENT relationship")
public class ConvertColumnMapsToJSON extends AbstractColumnMapProcessor {

	protected List<PropertyDescriptor> properties;
	protected Set<Relationship> relationships;

	private ObjectMapper objectMapper;

	public static final PropertyDescriptor SQL_DATE_FORMAT = new PropertyDescriptor.Builder()
		.name("java.sql.Date format")
		.defaultValue("yyyy-MM-dd")
		.description("Date format for serializing instances of java.sql.Date")
		.addValidator(Validator.VALID)
		.build();

	protected static final Relationship SUCCESS = new Relationship.Builder()
		.name("SUCCESS")
		.description("The incoming list of column maps is written to this relationship")
		.build();

	protected static final Relationship CONTENT = new Relationship.Builder()
		.name("CONTENT")
		.description("Each JSON document is written to this relationship")
		.build();

	@Override
	public void init(final ProcessorInitializationContext context) {
		List<PropertyDescriptor> list = new ArrayList<>();
		list.add(SQL_DATE_FORMAT);
		properties = Collections.unmodifiableList(list);

		Set<Relationship> set = new LinkedHashSet<>();
		set.add(SUCCESS);
		set.add(CONTENT);
		relationships = Collections.unmodifiableSet(set);
	}

	/**
	 * Iterates over each column map in the incoming list, serializes it to JSON, and creates a new FlowFile for it
	 * that is sent to the CONTENT relationship. The incoming list of column maps is then sent to the SUCCESS
	 * relationship.
	 *
	 * @param context
	 * @param session
	 * @throws ProcessException
	 */
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile != null) {
			List<Map<String, Object>> columnMapList = deserializeColumnMapList(session, flowFile);
			getLogger().info("Number of column maps received: " + columnMapList.size());

			for (Map<String, Object> columnMap : columnMapList) {
				createNewFlowFileForColumnMap(session, columnMap);
			}

			flowFile = session.write(flowFile, new ColumnMapsWriter(columnMapList));
			session.transfer(flowFile, SUCCESS);
		}
	}

	@OnScheduled
	public void initializeObjectMapper(ProcessContext context) {
		getLogger().info("Initializing Jackson ObjectMapper");
		objectMapper = new ObjectMapper();

		final String sqlDateFormat = context.getProperty(SQL_DATE_FORMAT).getValue();
		if (sqlDateFormat != null) {
			getLogger().info("Using format for serializing instances of java.sql.Date: " + sqlDateFormat);
			SimpleModule simpleModule = new SimpleModule();
			simpleModule.addSerializer(Date.class, new SqlDateSerializer(sqlDateFormat));
			objectMapper.registerModule(simpleModule);
		}
	}

	/**
	 * Serialize the given column map to JSON and write it to a new FlowFile.
	 *
	 * @param session
	 * @param columnMap
	 */
	protected void createNewFlowFileForColumnMap(ProcessSession session, Map<String, Object> columnMap) {
		final String jsonToWrite = serializeColumnMap(columnMap);
		FlowFile newFlowFile = session.create();
		session.write(newFlowFile, out -> out.write(jsonToWrite.getBytes()));
		session.transfer(newFlowFile, CONTENT);
	}

	protected String serializeColumnMap(Map<String, Object> columnMap) {
		try {
			return objectMapper.writeValueAsString(columnMap);
		} catch (JsonProcessingException e) {
			throw new ProcessException("Unable to write column map to JSON, cause: " + e.getMessage(), e);
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

	public ObjectMapper getObjectMapper() {
		return objectMapper;
	}
}
