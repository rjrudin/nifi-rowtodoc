package com.marklogic.nifi.processors.rowtodoc;

import org.apache.nifi.processor.io.OutputStreamCallback;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public class ColumnMapsWriter implements OutputStreamCallback {

	private List<Map<String, Object>> columnMaps;

	public ColumnMapsWriter(List<Map<String, Object>> columnMaps) {
		this.columnMaps = columnMaps;
	}

	@Override
	public void process(OutputStream out) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(columnMaps);
		out.write(baos.toByteArray());
	}
}
