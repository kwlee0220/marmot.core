package marmot.io.geo.quadtree;

import marmot.MarmotInternalException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class QuadTreeException extends MarmotInternalException {
	private static final long serialVersionUID = 1881855135584709862L;

	public QuadTreeException(String details) {
		super(details);
	}
}
