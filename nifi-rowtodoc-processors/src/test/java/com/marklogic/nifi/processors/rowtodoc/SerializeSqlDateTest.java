package com.marklogic.nifi.processors.rowtodoc;

import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessorInitializationContext;
import org.junit.Before;
import org.junit.Test;

import java.sql.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SerializeSqlDateTest {

	private ConvertColumnMapsToJSON processor;
	private MockProcessContext processContext;
	private MockProcessorInitializationContext initializationContext;

	@Before
	public void setup() {
		processor = new ConvertColumnMapsToJSON();
		processContext = new MockProcessContext(processor);
		initializationContext = new MockProcessorInitializationContext(processor, processContext);
		processor.initialize(initializationContext);
	}

	@Test
	public void customFormat() {
		verifyFormattedDate("MM/dd/yyyy", "09/01/2018");
	}

	@Test
	public void defaultFormat() {
		verifyFormattedDate(null, "2018-09-01");
	}

	private void verifyFormattedDate(String pattern, String expectedValue) {
		if (pattern != null) {
			processContext.setProperty(ConvertColumnMapsToJSON.SQL_DATE_FORMAT, pattern);
		}

		processor.initializeObjectMapper(processContext);

		Map<String, Object> columnMap = new HashMap<>();
		final long septemberFirst2018 = 1535836481720l;
		columnMap.put("date", new Date(septemberFirst2018));

		String json = processor.serializeColumnMap(columnMap);
		assertEquals(String.format("{\"date\":\"%s\"}", expectedValue), json);
	}
}
