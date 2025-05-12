package marmot.io.geo.cluster;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import marmot.GRecordSchema;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.CoordinateTransform;
import marmot.support.EnvelopeTaggedRecord;
import marmot.type.MapTile;

import utils.Tuple;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class InMemoryQuadClusterFile implements QuadClusterFile<InMemoryQuadCluster> {
	@SuppressWarnings("unused")
	private static final Logger s_logger = LoggerFactory.getLogger(InMemoryQuadClusterFile.class);
	
	private final GRecordSchema m_gschema;
	private final Map<String,InMemoryQuadCluster> m_clusters;
	private final Envelope m_quadBounds;
	private final Envelope m_dataBounds;
	private final long m_recordCount;
	private final long m_duplicateCount;

	InMemoryQuadClusterFile(GRecordSchema gschema, Map<String,InMemoryQuadCluster> clusters) {
		m_gschema =  gschema;
		m_clusters = clusters;

		m_quadBounds = new Envelope();
		m_dataBounds = new Envelope();
		int recordCount = 0;
		int duplicateCount = 0;
		
		for ( InMemoryQuadCluster cluster: clusters.values() ) {
			m_quadBounds.expandToInclude(cluster.getQuadBounds());
			m_dataBounds.expandToInclude(cluster.getDataBounds());
			recordCount += cluster.getRecordCount();
			duplicateCount += cluster.getDuplicateCount();
		}
		
		m_recordCount = recordCount;
		m_duplicateCount = duplicateCount;
	}

	@Override
	public GRecordSchema getGRecordSchema() {
		return m_gschema;
	}

	@Override
	public Envelope getDataBounds() {
		return m_dataBounds;
	}

	@Override
	public Envelope getQuadBounds() {
		return m_quadBounds;
	}

	@Override
	public long getRecordCount() {
		return m_recordCount;
	}

	@Override
	public long getDuplicateCount() {
		return m_duplicateCount;
	}

	@Override
	public int getClusterCount() {
		return m_clusters.size();
	}

	@Override
	public InMemoryQuadCluster getCluster(String clusterQuadKey) {
		return m_clusters.get(clusterQuadKey);
	}

	@Override
	public Set<String> getClusterKeyAll() {
		return m_clusters.keySet();
	}

	@Override
	public FStream<String> queryClusterKeys(Envelope range84) {
		Utilities.checkNotNullArgument(range84, "range is null");
		
		return FStream.from(m_clusters.values())
						.filter(cluster -> cluster.getQuadBounds().intersects(range84))
						.map(cluster -> cluster.getQuadKey());
	}
	
	public static InMemoryQuadClusterFile build(GeometryColumnInfo gcInfo, RecordSet rset,
												Iterable<String> quadKeys) {
		List<Tuple<String,Envelope>> tiles
						= FStream.from(quadKeys)
									.map(qk -> Tuple.of(qk, MapTile.fromQuadKey(qk).getBounds()))
									.toList();
		Map<String,InMemoryQuadCluster> clusters = Maps.newHashMap();
		
		GRecordSchema gschema = new GRecordSchema(gcInfo, rset.getRecordSchema());
		int geomColIdx = rset.getRecordSchema().getColumn(gcInfo.name()).ordinal();
		CoordinateTransform trans = CoordinateTransform.getTransformToWgs84(gcInfo.srid());
		
		Record record;
		while ( (record = rset.nextCopy()) != null ) {
			Geometry geom = record.getGeometry(geomColIdx);
			Envelope mbr = geom.getEnvelopeInternal();
			if ( trans != null ) {
				mbr = trans.transform(mbr);
			}
			EnvelopeTaggedRecord etr = new EnvelopeTaggedRecord(mbr, record);
			
			RecordSchema schema = rset.getRecordSchema();
			FStream.from(tiles)
					.filter(t -> t._2.intersects(etr.getEnvelope()))
					.forEach(t -> clusters.computeIfAbsent(t._1, k -> new InMemoryQuadCluster(k, gschema))
											.add(geom, etr));
		}
		
		return new InMemoryQuadClusterFile(gschema, clusters);
	}
}
