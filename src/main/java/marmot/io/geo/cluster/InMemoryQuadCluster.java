package marmot.io.geo.cluster;

import java.util.List;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import com.google.common.collect.Lists;

import marmot.GRecordSchema;
import marmot.io.geo.quadtree.Pointer;
import marmot.io.geo.quadtree.PointerPartition;
import marmot.io.geo.quadtree.QuadTree;
import marmot.support.EnvelopeTaggedRecord;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class InMemoryQuadCluster extends QuadTreeSpatialCluster implements CacheableQuadCluster {
	private final Envelope m_dataBounds;
	private long m_duplicateCount;
	
	private QuadTree<Pointer,PointerPartition> m_qtree;
	private final List<EnvelopeTaggedRecord> m_records;

	InMemoryQuadCluster(String quadKey, GRecordSchema gschema) {
		super(quadKey, gschema);
		
		m_dataBounds = new Envelope();
		m_records = Lists.newArrayList();
		m_qtree = new QuadTree<>(quadKey, qkey->new PointerPartition());
		
		m_duplicateCount = 0;
	}

	InMemoryQuadCluster(String quadKey, GRecordSchema schema, List<EnvelopeTaggedRecord> records) {
		this(quadKey, schema);
		
		for ( EnvelopeTaggedRecord etr: records ) {
			Geometry geom = etr.getRecord().getGeometry(m_geomColIdx);
			add(geom, etr);
		}
	}

	@Override
	public Envelope getDataBounds() {
		return m_dataBounds;
	}

	@Override
	public long getDuplicateCount() {
		return m_duplicateCount;
	}

	@Override
	public int length() {
		return 0;
	}

	@Override
	protected QuadTree<Pointer, PointerPartition> getQuadTree() {
		return m_qtree;
	}

	@Override
	protected List<EnvelopeTaggedRecord> getRecordList() {
		return m_records;
	}
	
	void add(Geometry geom, EnvelopeTaggedRecord etr) {
		m_dataBounds.expandToInclude(geom.getEnvelopeInternal());
		m_records.add(etr);

		if ( isOwnerOf(etr.getEnvelope()) ) {
			++m_duplicateCount;
		}

		Envelope mbr = (isOutlier()) ? new Envelope() : toWgs84(geom.getEnvelopeInternal());
		m_qtree.insert(new Pointer(mbr, m_records.size()-1));
	}
}
