package marmot.geo.javageom;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface OgcGeometry {
	public enum Type {
		POINT,
		MULTI_POINT,
		LINESTRING,
		MULTI_LINESTRING,
		POLYGON,
		MULTI_POLYGON
	};
	
	public Type getType();
	public boolean isEmpty();
	
	public OgcGeometry intersection(OgcGeometry geom);
}
