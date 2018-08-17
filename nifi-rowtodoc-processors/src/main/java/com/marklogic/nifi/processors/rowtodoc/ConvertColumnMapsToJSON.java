package com.marklogic.nifi.processors.rowtodoc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.*;

/**
 * Converts each column map in a list (a List<Map<String, Object>>) into a JSON document.
 */
public class ConvertColumnMapsToJSON extends AbstractColumnMapProcessor {

	protected List<PropertyDescriptor> properties;
	protected Set<Relationship> relationships;

	// TODO Pull this from a controller service?
	private ObjectMapper objectMapper = new ObjectMapper();

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

	/**
	 * Serialize the given column map to JSON and write it to a new FlowFile.
	 *
	 * @param session
	 * @param columnMap
	 */
	protected void createNewFlowFileForColumnMap(ProcessSession session, Map<String, Object> columnMap) {
		String documentJson;
		try {
			documentJson = objectMapper.writeValueAsString(columnMap);
		} catch (JsonProcessingException e) {
			throw new ProcessException("Unable to write column map to JSON, cause: " + e.getMessage(), e);
		}
		final String jsonToWrite = documentJson;
		FlowFile newFlowFile = session.create();
		session.write(newFlowFile, out -> out.write(jsonToWrite.getBytes()));
		session.transfer(newFlowFile, CONTENT);
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
