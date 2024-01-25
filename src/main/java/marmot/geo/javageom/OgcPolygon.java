package marmot.geo.javageom;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface OgcPolygon extends OgcGeometry {
	public default Type getType() {
		return Type.POLYGON;
	}

	public OgcLinearRing getShell();
}
