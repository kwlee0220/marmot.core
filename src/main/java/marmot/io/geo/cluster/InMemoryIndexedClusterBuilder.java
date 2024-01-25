package marmot.io.geo.cluster;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.concurrent.GuardedBy;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import marmot.GRecordSchema;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.CoordinateTransform;
import marmot.io.geo.quadtree.EnvelopedValue;
import marmot.io.geo.quadtree.QuadTree;
import marmot.io.geo.quadtree.SimplePartition;
import marmot.support.EnvelopeTaggedRecord;
import marmot.type.MapTile;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class InMemoryIndexedClusterBuilder implements Supplier<InMemoryQuadClusterFile>, Runnable {
	private final RecordSet m_rset;
	private final GRecordSchema m_gschema;
	private final String m_geomCol;
	private final CoordinateTransform m_trans;
	private final int m_clusterCountHint;
	@GuardedBy("this") private InMemoryQuadClusterFile m_index = null;
	
	public InMemoryIndexedClusterBuilder(GeometryColumnInfo gcInfo, RecordSet rset,
											int clusterCountHint) {
		m_rset = rset;
		m_gschema = new GRecordSchema(gcInfo, rset.getRecordSchema());
		m_geomCol = gcInfo.name();
		m_trans = CoordinateTransform.get(gcInfo.srid(), "EPSG:4326");
		m_clusterCountHint = clusterCountHint;
	}

	@Override
	public synchronized InMemoryQuadClusterFile get() {
		if ( m_index == null ) {
			run();
		}
		
		return m_index;
	}

	int m_clusterSizeGuess;
	@Override
	public void run() {
		RecordSchema dataSchema = m_rset.getRecordSchema();
		int geomIdx = dataSchema.getColumn(m_geomCol).ordinal();

		// QuadTree를 만들기 위해 최상위 tile의 quad-key를 구한다.
		//
		Envelope dataBounds = new Envelope();
		List<Record> dataset = m_rset.fstream()
									.filter(r -> {
										Geometry geom = r.getGeometry(geomIdx);
										if ( geom == null || geom.isEmpty() ) {
											// geometry 정보가 없는 경우는 무시한다.
											return false;
										}
										
										dataBounds.expandToInclude(geom.getEnvelopeInternal());
										return true;
									})
									.toList();
		m_rset.closeQuietly();
		
		Envelope boundsWgs84 = m_trans.transform(dataBounds);
		MapTile mbTile = MapTile.getSmallestContainingTile(boundsWgs84);
		
		// 주어진 개수의 cluster가 생성되도록 partition당 레코드 갯수를 추정하여 QuadTree를 설정한다.
		int guessCount = Math.max(1, Math.min(m_clusterCountHint, dataset.size()/256));
		m_clusterSizeGuess = (int)Math.round(dataset.size() / (guessCount*0.5));
		while ( true ) {
			QuadTree<IMValue,IMPartition> qtree = new QuadTree<>(mbTile.getQuadKey(),
																qkey->new IMPartition(m_clusterSizeGuess));
			qtree.setRangeExpandable(false);
		
			// 모든 레코드를 QuadTree에 삽입한 뒤, 생성된 leafnode별로 cluster를 생성시킨다.
			dataset.forEach(rec -> qtree.insert(new IMValue(rec)));
			
			long nclusters = qtree.streamLeafNodes().count();
			if ( nclusters <= m_clusterCountHint*2 ) {
				Map<String, InMemoryQuadCluster> clusters
					= qtree.streamLeafNodes()
						.map(node -> node.getPartition().toCluster(node.getQuadKey(), m_gschema))
						.toMap(InMemoryQuadCluster::getQuadKey, Function.identity());
				
				// 생성된 cluster로부터 cluster index를 생성한다.
				synchronized ( this ) {
					m_index = new InMemoryQuadClusterFile(m_gschema, clusters);
				}
				
				return;
			}
			
			m_clusterSizeGuess *= 2;
		}
	}
	
	class IMValue implements EnvelopedValue {
		private final Envelope m_mbr;
		private final Record m_record;
		
		IMValue(Record record) {
			m_record = record;
			m_mbr = m_trans.transform(m_record.getGeometry(m_geomCol).getEnvelopeInternal());
		}

		@Override
		public Envelope getEnvelope() {
			return m_mbr;
		}
		
		EnvelopeTaggedRecord toTaggedRecord() {
			return new EnvelopeTaggedRecord(m_mbr, m_record);
		}
	}
	
	class IMPartition extends SimplePartition<IMValue> {
		IMPartition(int maxCount) {
			super(maxCount);
		}
		
		InMemoryQuadCluster toCluster(String quadKey, GRecordSchema gschema) {
			int geomColIdx = gschema.getGeometryColumnIdx();
			InMemoryQuadCluster cluster = new InMemoryQuadCluster(quadKey, gschema);
			values().map(IMValue::toTaggedRecord)
					.forEach(etr -> cluster.add(etr.getRecord().getGeometry(geomColIdx), etr));
			return cluster;
		}
	}
}
