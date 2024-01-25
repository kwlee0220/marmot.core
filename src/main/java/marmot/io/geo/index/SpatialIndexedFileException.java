package marmot.io.geo.index;

import marmot.MarmotInternalException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialIndexedFileException extends MarmotInternalException {
	private static final long serialVersionUID = 1L;

	public SpatialIndexedFileException(String details) {
		super(details);
	}
}
