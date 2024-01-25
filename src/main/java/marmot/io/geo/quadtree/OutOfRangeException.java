package marmot.io.geo.quadtree;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class OutOfRangeException extends QuadTreeException {
	private static final long serialVersionUID = -8259952813176127032L;
	
	public OutOfRangeException(String details) {
		super(details);
	}
}
