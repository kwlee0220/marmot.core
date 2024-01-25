package marmot.optor.geo.join;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.Record;
import marmot.geo.CoordinateTransform;
import marmot.io.geo.cluster.QuadCluster;
import marmot.optor.support.Match;
import marmot.support.EnvelopeTaggedRecord;
import utils.LoggerSettable;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractSpatialJoinMatcher implements SpatialJoinMatcher, LoggerSettable {
	private static final Logger s_logger = LoggerFactory.getLogger(AbstractSpatialJoinMatcher.class);
	
	protected int m_outerGeomColIdx = -1; 
	protected int m_innerGeomColIdx = -1;
	private CoordinateTransform m_trans;
	private Logger m_logger = s_logger;
	
	protected abstract FStream<EnvelopeTaggedRecord> match(Geometry outerGeom, Record outer,
															SpatialLookupTable innerCluster);

	@Override
	public void open(int outerGeomColIdx, int innerGeomColIdx, String srid) {
		m_outerGeomColIdx = outerGeomColIdx;
		m_innerGeomColIdx = innerGeomColIdx;
		
		m_trans = (!srid.equals("EPSG:4326"))
				? CoordinateTransform.get(srid, "EPSG:4326") : null;
	}
	@Override public void close() { }

	@Override
	public FStream<EnvelopeTaggedRecord> match(Record outer, SpatialLookupTable slut) {
		Geometry outerGeom = outer.getGeometry(m_outerGeomColIdx);
		if ( outerGeom == null || outerGeom.isEmpty() ) {
			return FStream.empty();
		}
		
		return match(outerGeom, outer, slut);
	}
	
	@Override
	public Logger getLogger() {
		return m_logger;
	}
	
	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}
	
	@Override
	public String toString() {
		return toStringExpr();
	}

	public FStream<Match<EnvelopeTaggedRecord>> match(QuadCluster outerCluster,
														QuadCluster innerCluster) {
		if ( getLogger().isDebugEnabled() ) {
			getLogger().debug(String.format("spatial cluster join: matcher=%s, %s(%d)<->%s(%d)",
											this,
											outerCluster.getQuadKey(), outerCluster.getRecordCount(),
											innerCluster.getQuadKey(), innerCluster.getRecordCount()));
		}
		
		boolean swap = outerCluster.getRecordCount() > innerCluster.getRecordCount();
		Envelope ownership = (innerCluster.getQuadKey().startsWith(outerCluster.getQuadKey()))
							? innerCluster.getQuadBounds() : outerCluster.getQuadBounds();	

		FStream<Match<EnvelopeTaggedRecord>> matcheds;
		if ( !swap ) {			
			if ( getLogger().isDebugEnabled() ) {
				getLogger().debug("match: outer({}:{}) <-> inner({}:{})",
											outerCluster.getQuadKey(), outerCluster.getRecordCount(),
											innerCluster.getQuadKey(), innerCluster.getRecordCount());
			}
			
			Envelope range = innerCluster.getDataBounds();
			matcheds = outerCluster.query(range, true)	// inner 영역과 겹치지 않는 레코드는 제외
									.flatMap(outer -> match(outer.getRecord(), innerCluster)
										 				.map(inner -> new Match<>(outer,inner)));
		}
		else {
			// swap하는 경우는 일단 match는 outer/inner를 바꾸어서 수행하고
			// 매치된 결과 pair을 순서를 원래대로 바꾼다.
			if ( getLogger().isDebugEnabled() ) {
				getLogger().debug("match(swapped): outer({}:{}) <-> inner({}:{})",
								innerCluster.getQuadKey(), innerCluster.getRecordCount(),
								outerCluster.getQuadKey(), outerCluster.getRecordCount());
			}

			Envelope range = outerCluster.getDataBounds();
			matcheds = innerCluster.query(range, true)
									.flatMap(inner -> match(inner.getRecord(), outerCluster)
														.map(outer -> new Match<>(outer,inner)));
		}
		
		return matcheds.filter(match -> {	// 중복을 제거하는 기능 수행
			Envelope leftEnvl = match.m_left.getEnvelope();
			Envelope rightEnvl = match.m_right.getEnvelope();
			
			Coordinate refPt = leftEnvl.intersection(rightEnvl).centre();
			return ownership.contains(refPt);
		});
	}
	
	protected Envelope toWgs84(Envelope envl) {
		return (m_trans != null) ? m_trans.transform(envl) : envl;
	}
}
