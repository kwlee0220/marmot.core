package marmot.geo.javageom;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface OgcMultiPolygon extends OgcMultiGeometry {
	public default Type getType() {
		return Type.MULTI_POLYGON;
	}
}
