package marmot.io.geo.cluster;

import java.util.List;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;

import marmot.GRecordSchema;
import marmot.Record;
import marmot.geo.CoordinateTransform;
import marmot.geo.GeoClientUtils;
import marmot.io.geo.quadtree.Pointer;
import marmot.io.geo.quadtree.PointerPartition;
import marmot.io.geo.quadtree.QuadTree;
import marmot.optor.geo.SpatialRelation;
import marmot.support.EnvelopeTaggedRecord;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class QuadTreeSpatialCluster extends AbstractQuadCluster {
	protected final int m_geomColIdx;
	private final CoordinateTransform m_trans;
	
	abstract protected QuadTree<Pointer,PointerPartition> getQuadTree();
	abstract protected List<EnvelopeTaggedRecord> getRecordList();

	protected QuadTreeSpatialCluster(String quadKey, GRecordSchema gschema) {
		super(quadKey, gschema);
		
		m_geomColIdx = gschema.getGeometryColumnIdx();
		m_trans = CoordinateTransform.getTransformToWgs84(gschema.getSrid());
	}

	@Override
	public long getRecordCount() {
		return getRecordList().size();
	}

	@Override
	public FStream<EnvelopeTaggedRecord> read(boolean dropDuplicates) {
		FStream<EnvelopeTaggedRecord> strm = FStream.from(getRecordList());
		if ( !isOutlier() && dropDuplicates ) {
			strm = strm.filter(etr -> isOwnerOf(etr.getEnvelope()));
		}
		return strm;
	}

	@Override
	public FStream<EnvelopeTaggedRecord> query(Envelope range84, boolean dropDuplicates) {
		List<EnvelopeTaggedRecord> records = getRecordList();
		FStream<EnvelopeTaggedRecord> strm =  getQuadTree().query(SpatialRelation.INTERSECTS, range84)
															.distinct()
															.map(ptr -> records.get(ptr.index()));
		if ( !isOutlier() && dropDuplicates ) {
			strm = strm.filter(etr -> isOwnerOf(etr.getEnvelope().intersection(range84)));
		}
		return strm;
	}
	
	public FStream<Record> queryRecord(Envelope range, boolean dropDuplicates) {
		if ( isOutlier() ) {
			return FStream.empty();
		}
		
		PreparedGeometry pkey = PreparedGeometryFactory.prepare(GeoClientUtils.toPolygon(range));
		FStream<EnvelopeTaggedRecord> strm
								= FStream.from(getRecordList())
										.filter(etr -> {
											Geometry geom = etr.getRecord().getGeometry(m_geomColIdx);
											return pkey.intersects(geom);
										});
		if ( dropDuplicates ) {
			Envelope range84 = toWgs84(range);
			Envelope quadBounds = getQuadBounds();
			
			strm = strm.filter(etr -> {
				// duplication 여부는 공간 객체를 EPSG:4326좌표계로 변환하여 확인한다.
				
				Envelope overlap = range84.intersection(etr.getEnvelope());
				// EPSG:4326 좌표계에서는 겹치지 않는 경우도 발생하기 때문에 한번 더 확인한다.
				if ( overlap.isNull() ) {
					return false;
				}
				return quadBounds.contains(overlap.centre());
			});
		}
		
		return strm.map(EnvelopeTaggedRecord::getRecord);
	}
	
	protected Envelope toWgs84(Envelope envl) {
		return (m_trans != null) ? m_trans.transform(envl) : envl;
	}
}
