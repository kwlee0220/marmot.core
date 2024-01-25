package marmot.io.serializer;

import marmot.MarmotInternalException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SerializationException extends MarmotInternalException {
	private static final long serialVersionUID = -2758205514741221749L;

	public SerializationException(String details) {
		super(details);
	}

	public SerializationException(String details, Throwable cause) {
		super(details, cause);
	}
}
