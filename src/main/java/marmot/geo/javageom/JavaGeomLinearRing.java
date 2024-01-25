package marmot.geo.javageom;

import math.geom2d.polygon.LinearRing2D;
import math.geom2d.polygon.MultiPolygon2D;
import math.geom2d.polygon.Polygon2D;
import math.geom2d.polygon.Polygons2D;
import math.geom2d.polygon.SimplePolygon2D;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JavaGeomLinearRing implements OgcLinearRing {
	private final LinearRing2D m_ring;
	
	public JavaGeomLinearRing(LinearRing2D ring) {
		m_ring = ring;
	}

	@Override
	public boolean isEmpty() {
		return m_ring == null;
	}
	
	public LinearRing2D asJavaGeom() {
		return m_ring;
	}

	@Override
	public OgcGeometry intersection(OgcGeometry geom) {
		if ( m_ring == null ) {
			return this;
		}
		
		switch ( geom.getType() ) {
			case POINT:
				return intersectsPoint((OgcPoint)geom);
			case LINESTRING:
				if ( geom instanceof OgcLinearRing ) {
					return intersectsLinearRing((OgcLinearRing)geom);
				}
				break;
			case POLYGON:
				return geom.intersection(this);
		}
		
		throw new UnsupportedOperationException();
	}
	
	private OgcGeometry intersectsPoint(OgcPoint pt) {
		if ( !m_ring.contains(pt.getX(), pt.getY()) ) {
			return new JavaGeomPoint(pt.getX(), pt.getY());
		}
		else {
			return new JavaGeomPoint();
		}
	}
	
	private OgcGeometry intersectsLinearRing(OgcLinearRing ring) {
		if ( m_ring == null || ring.isEmpty() ) {
			return this;
		}
		else if ( !(ring instanceof JavaGeomLinearRing) ) {
			throw new IllegalArgumentException("ring is not a JavaGeomLinearRing: ring=" + ring);
		}
		
		JavaGeomLinearRing other = (JavaGeomLinearRing)ring;
		Polygon2D result = Polygons2D.intersection(new SimplePolygon2D(m_ring),
													new SimplePolygon2D(other.m_ring));
		if ( result instanceof SimplePolygon2D ) {
			return new JavaGeomLinearRing(((SimplePolygon2D)result).getRing());
		}
		else {
			return new JavaGeomMultiPolygon((MultiPolygon2D)result);
		}
	}
}
