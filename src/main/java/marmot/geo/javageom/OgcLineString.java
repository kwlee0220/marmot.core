package marmot.geo.javageom;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface OgcLineString  extends OgcGeometry {
	public default Type getType() {
		return Type.LINESTRING;
	}
}
