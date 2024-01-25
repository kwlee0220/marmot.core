package marmot.geo.javageom;

import marmot.MarmotInternalException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JavaGeomException extends MarmotInternalException {
	private static final long serialVersionUID = -8031856896795493575L;

	public JavaGeomException(String details) {
		super(details);
	}
}
