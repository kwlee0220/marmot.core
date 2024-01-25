package marmot.optor.support;

import java.util.Arrays;
import java.util.Collection;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Geometry;

import marmot.support.GeoUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SafeUnion {
	private Geometries m_resultType;
	
	public SafeUnion(Geometries resultType) {
		m_resultType = resultType;
	}
	
	public Geometries getResultType() {
		return m_resultType;
	}
	
	public void setResultType(Geometries type) {
		m_resultType = type;
	}
	
	public Geometry apply(Geometry... geoms) {
		return apply(GeoUtils.toGeometryCollection(Arrays.asList(geoms)));
	}
	
	public Geometry apply(Collection<Geometry> geoms) {
		if ( geoms.size() == 1 ) {
			Geometry result = geoms.iterator().next();
			if ( m_resultType != null ) {
				result = GeoUtils.cast(result, m_resultType);
			}
			
			return result;
		}
		else {
			return apply(GeoUtils.toGeometryCollection(geoms));
		}
	}
	
	public Geometry apply(Geometry geom) {
		Geometry result;
		try {
			result = geom.union();
		}
		catch ( Throwable e ) {
			result = geom.buffer(0);
			if ( result.isEmpty() ) {
				throw new RuntimeException("Geometry becomes NULL");
			}
		}
		
		if ( m_resultType != null ) {
			result = GeoUtils.cast(result, m_resultType);
		}
		
		return result;
	}
}
