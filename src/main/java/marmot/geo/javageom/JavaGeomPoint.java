package marmot.geo.javageom;

import com.google.common.base.Preconditions;

import math.geom2d.Point2D;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JavaGeomPoint implements OgcPoint {
	private final Point2D m_pt;
	
	public JavaGeomPoint() {
		m_pt = null;
	}
	
	public JavaGeomPoint(Point2D ring) {
		m_pt = ring;
	}
	
	public JavaGeomPoint(double x, double y) {
		m_pt = new Point2D(x,y);
	}
	
	public Point2D asJavaGeom() {
		return m_pt;
	}

	@Override
	public boolean isEmpty() {
		return m_pt != null;
	}

	@Override
	public double getX() {
		Preconditions.checkState(m_pt != null, "empty JavaGeomPoint");
		return m_pt.getX();
	}

	@Override
	public double getY() {
		Preconditions.checkState(m_pt != null, "empty JavaGeomPoint");
		
		return m_pt.getY();
	}

	@Override
	public OgcGeometry intersection(OgcGeometry geom) {
		throw new UnsupportedOperationException();
	}
}
