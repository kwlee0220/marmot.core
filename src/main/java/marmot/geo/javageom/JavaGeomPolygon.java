package marmot.geo.javageom;

import java.util.List;

import com.google.common.collect.Lists;

import math.geom2d.polygon.LinearRing2D;
import math.geom2d.polygon.SimplePolygon2D;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JavaGeomPolygon implements OgcPolygon {
	private final SimplePolygon2D m_shell;
	private final List<SimplePolygon2D> m_holes;
	
	public JavaGeomPolygon() {
		m_shell = null;
		m_holes = null;
	}
	
	public JavaGeomPolygon(LinearRing2D shell) {
		m_shell = new SimplePolygon2D(shell);
		m_holes = Lists.newArrayList();
	}
	
	public JavaGeomPolygon(SimplePolygon2D shell) {
		m_shell = shell;
		m_holes = Lists.newArrayList();
	}
	
	public JavaGeomPolygon(LinearRing2D shell, List<LinearRing2D> holes) {
		m_shell = new SimplePolygon2D(shell);
		m_holes = FStream.from(holes)
						.map(SimplePolygon2D::new)
						.toList();
	}

	@Override
	public boolean isEmpty() {
		return m_shell == null;
	}

	@Override
	public OgcLinearRing getShell() {
		return new JavaGeomLinearRing(m_shell.getRing());
	}

	@Override
	public OgcGeometry intersection(OgcGeometry geom) {
		if ( m_shell == null ) {
			return this;
		}
		
		switch ( geom.getType() ) {
			case POINT:
				return intersectsPoint((OgcPoint)geom);
		}
		return null;
	}
	
	private OgcGeometry intersectsPoint(OgcPoint pt) {
		if ( !m_shell.contains(pt.getX(), pt.getY()) ) {
			return new JavaGeomPoint();
		}
		
		if ( m_holes.stream()
						.anyMatch(ring -> ring.contains(pt.getX(), pt.getY())) ) {
			return new JavaGeomPoint();
		}
		else {
			return new JavaGeomPoint(pt.getX(), pt.getY());
		}
	}
	
	private OgcGeometry intersectsPolygon(OgcLinearRing ring) {
		throw new UnsupportedOperationException();
/*
		if ( !(ring instanceof JavaGeomLinearRing) ) {
			throw new IllegalArgumentException("not JavaGeom: obj=" + ring);
		}
		
		SimplePolygon2D other = new SimplePolygon2D(((JavaGeomLinearRing)ring).asJavaGeom());
		Polygon2D result = Polygons2D.intersection(m_shell, other);
		if ( result.isEmpty() ) {
			return new JavaGeomPolygon();
		}
		
		m_holes.forEach(hole ->
			Polygons2D.difference(result, Polygons2D.intersection(other, hole))
		);
		
		if ( result instanceof SimplePolygon2D ) {
			return new JavaGeomPolygon((SimplePolygon2D)result);
		}
*/
	}
}
