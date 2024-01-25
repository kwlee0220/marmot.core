package marmot.optor.support;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.precision.GeometryPrecisionReducer;

import marmot.geo.GeoClientUtils;
import marmot.geo.javageom.JavaGeomException;
import marmot.geo.javageom.JavaGeoms;
import marmot.support.GeoUtils;
import marmot.type.DataType;
import marmot.type.GeometryDataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SafeIntersection {
	private Geometries m_resultType;
	private GeometryPrecisionReducer m_reducer = null;
	
	public SafeIntersection() {
		this(null);
	}
	
	public SafeIntersection(Geometries resultType) {
		m_resultType = resultType;
	}
	
	public void setResultType(Geometries type) {
		m_resultType = type;
	}
	
	public SafeIntersection setReduceFactor(int factor) {
		m_reducer = (factor >= 0) ? GeoClientUtils.toGeometryPrecisionReducer(factor) : null;
		return this;
	}
	
	public Geometry apply(Geometry geom1, Geometry geom2) {
		Geometry result = null;
		try {
			result = geom1.intersection(geom2);
		}
		catch ( Exception e ) {
			try {
				result = JavaGeoms.intersection(geom1, geom2);
				if ( result == null ) {
					throw e;
				}
			}
			catch ( JavaGeomException ignored ) {
				result = intersectionWithPrecision(geom1, geom2);
			}
		}
		
		if ( m_resultType != null ) {
			result = GeoUtils.cast(result, m_resultType);
		}
		
		return result;
	}
	
	public static DataType calcOutputType(DataType left, DataType right) {
		if ( left.getTypeCode() == right.getTypeCode() ) {
			return left;
		}
		
		if ( left.getTypeCode().get() < right.getTypeCode().get() ) {
			DataType tmp = left;
			left = right;
			right = tmp;
		}
		switch ( left.getTypeCode() ) {
			case MULTI_POLYGON:
				return right;
		}
		
		return DataType.GEOM_COLLECTION;
	}
	
	private Geometry intersectionWithPrecision(Geometry geom1, Geometry geom2) {
		if ( m_reducer == null ) {
			return GeometryDataType.fromGeometries(m_resultType).newInstance();
		}
		
		geom1 = m_reducer.reduce(geom1);
		geom2 = m_reducer.reduce(geom2);
		
		return geom1.intersection(geom2);
	}
}
