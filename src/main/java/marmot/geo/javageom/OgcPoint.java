package marmot.geo.javageom;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface OgcPoint extends OgcGeometry {
	public default Type getType() {
		return Type.POINT;
	}
	
	public double getX();
	public double getY();
}
