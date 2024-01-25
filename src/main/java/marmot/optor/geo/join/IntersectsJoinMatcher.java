package marmot.optor.geo.join;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
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
public class IntersectsJoinMatcher extends AbstractSpatialJoinMatcher {
	private static final Logger s_logger = LoggerFactory.getLogger(IntersectsJoinMatcher.class);

	public IntersectsJoinMatcher() {
		setLogger(s_logger);
	}
	
	@Override
	public SpatialRelation toSpatialRelation() {
		return SpatialRelation.INTERSECTS;
	}

	@Override
	public FStream<EnvelopeTaggedRecord> match(Geometry outerGeom, Record outer,
													SpatialLookupTable slut) {
		Envelope key = toMatchKey(outerGeom);
		PreparedGeometry pouter = PreparedGeometryFactory.prepare(outerGeom);

		return slut.query(key, true)
					.filter(inner -> {
						Geometry innerGeom = inner.getRecord().getGeometry(m_innerGeomColIdx);
						return pouter.intersects(innerGeom);
					});
	}

	@Override
	public Envelope toMatchKey(Geometry geom) {
		return toWgs84(geom.getEnvelopeInternal());
	}
	
	@Override
	public String toStringExpr() {
		return "intersects";
	}
}
