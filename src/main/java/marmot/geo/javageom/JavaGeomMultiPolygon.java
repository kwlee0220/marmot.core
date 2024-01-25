package marmot.geo.javageom;

import java.util.List;

import math.geom2d.polygon.MultiPolygon2D;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JavaGeomMultiPolygon implements OgcMultiPolygon {
	private final MultiPolygon2D m_mpoly;
	private List<JavaGeomPolygon> m_components;
	
	public JavaGeomMultiPolygon(MultiPolygon2D mpoly) {
		m_mpoly = mpoly;
	}

	@Override
	public boolean isEmpty() {
		return m_mpoly == null;
	}

	@Override
	public List<? extends OgcGeometry> getComponents() {
		return m_components;
	}

	@Override
	public OgcGeometry intersection(OgcGeometry geom) {
		throw new UnsupportedOperationException();
	}

}
