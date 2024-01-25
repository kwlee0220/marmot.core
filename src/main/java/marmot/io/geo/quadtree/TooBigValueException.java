package marmot.io.geo.quadtree;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TooBigValueException extends QuadTreeException {
	private static final long serialVersionUID = -8259952813176127032L;
	
	public TooBigValueException(String details) {
		super(details);
	}
}
