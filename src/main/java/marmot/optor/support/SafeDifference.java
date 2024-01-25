package marmot.optor.support;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.precision.GeometryPrecisionReducer;

import marmot.geo.GeoClientUtils;
import marmot.geo.javageom.JavaGeoms;
import marmot.support.GeoUtils;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SafeDifference {
	private FOption<Geometries> m_resultType = FOption.empty();
	private GeometryPrecisionReducer m_reducer = null;
	
	public SafeDifference() {
		m_resultType = FOption.empty();
	}
	
	public SafeDifference(Geometries resultType) {
		Utilities.checkNotNullArgument(resultType, "resultType is null");
		
		m_resultType = FOption.of(resultType);
	}
	
	public void setResultType(Geometries type) {
		m_resultType = FOption.of(type);
	}
	
	public SafeDifference setReduceFactor(int factor) {
		m_reducer = (factor >= 0) ? GeoClientUtils.toGeometryPrecisionReducer(factor) : null;
		return this;
	}
	
	public Geometry apply(Geometry geom1, Geometry geom2) {
		Geometry result = null;
		try {
			result = geom1.difference(geom2);
		}
		catch ( Exception e ) {
			try {
				result = JavaGeoms.difference(geom1, geom2);
				if ( result == null ) {
					throw e;
				}
			}
			catch ( Exception ignored ) {
				result = differenceWithPrecision(geom1, geom2);
			}
		}
		
		if ( result != null && !result.isEmpty() ) {
			Geometry cresult = result;
			result = m_resultType.map(resulType -> GeoUtils.cast(cresult, resulType))
								.getOrElse(result);
		}
		
		return result;
	}
	
	private Geometry differenceWithPrecision(Geometry geom1, Geometry geom2) {
		geom1 = m_reducer.reduce(geom1);
		geom2 = m_reducer.reduce(geom2);
		
		return geom1.difference(geom2);
	}
}
