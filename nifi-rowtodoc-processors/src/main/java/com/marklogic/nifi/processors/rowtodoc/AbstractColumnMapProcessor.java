package com.marklogic.nifi.processors.rowtodoc;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.Map;

public abstract class AbstractColumnMapProcessor extends AbstractProcessor {

	/**
	 * This processor expects the contents of the FlowFile to be a List<Map<String Object>> that was serialized via
	 * ObjectOutputStream.
	 *
	 * @param session
	 * @param flowFile
	 * @return
	 */
	protected List<Map<String, Object>> deserializeColumnMapList(ProcessSession session, FlowFile flowFile) {
		final byte[] content = new byte[(int) flowFile.getSize()];
		session.read(flowFile, inputStream -> StreamUtils.fillBuffer(inputStream, content));
		ByteArrayInputStream bais = new ByteArrayInputStream(content);
		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(bais);
			return (List<Map<String, Object>>) ois.readObject();
		} catch (Exception ex) {
			throw new ProcessException("Unable to read list of column maps from flow file, cause: " + ex.getMessage(), ex);
		} finally {
			if (ois != null) {
				try {
					ois.close();
				} catch (IOException e) {
					// ignore
				}
			}
		}
	}


}
