package marmot.optor.geo.join;

import static marmot.optor.geo.SpatialRelation.WITHIN_DISTANCE;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.Record;
import marmot.optor.geo.SpatialRelation;
import marmot.support.EnvelopeTaggedRecord;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class WithinDistanceJoinMatcher extends AbstractSpatialJoinMatcher {
	private static final Logger s_logger = LoggerFactory.getLogger(WithinDistanceJoinMatcher.class);
	
	private double m_distance;
	
	WithinDistanceJoinMatcher(double distance) {
		m_distance = distance;
		
		setLogger(s_logger);
	}
	
	@Override
	public SpatialRelation toSpatialRelation() {
		return WITHIN_DISTANCE(m_distance);
	}

	protected FStream<EnvelopeTaggedRecord> match(Geometry outerGeom, Record outer,
													SpatialLookupTable slut) {
		Envelope key = toMatchKey(outerGeom);
		return slut.query(key, true)
					.filter(inner -> {
						Geometry innerGeom = inner.getRecord().getGeometry(m_innerGeomColIdx);
						return outerGeom.isWithinDistance(innerGeom, m_distance);
					});
	}

	@Override
	public Envelope toMatchKey(Geometry geom) {
		Envelope envl = geom.getEnvelopeInternal();
		envl.expandBy(m_distance);
		
		return toWgs84(envl);
	}
	
	@Override
	public String toStringExpr() {
		return "within_distance(" + m_distance + ")";
	}
}
