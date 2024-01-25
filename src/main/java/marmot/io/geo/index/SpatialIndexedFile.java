package marmot.io.geo.index;

import java.io.IOException;
import java.util.Set;

import org.locationtech.jts.geom.Envelope;

import marmot.GRecordSchema;
import marmot.RecordSchema;
import marmot.io.HdfsPath;
import marmot.io.MarmotFileException;
import marmot.io.geo.cluster.QuadClusterFile;
import marmot.optor.support.Match;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialIndexedFile implements QuadClusterFile<SpatialIndexedCluster> {
	private final HdfsPath m_clusterDir;
	private final HdfsPath m_indexFilePath;
	private final GlobalIndex m_globalIdx;
	private final RecordSchema m_schema;
	
	public static SpatialIndexedFile load(HdfsPath clusterDir) {
		return new SpatialIndexedFile(clusterDir);
	}

	private SpatialIndexedFile(HdfsPath clusterDir) {
		try {
			if ( !clusterDir.isDirectory() ) {
				throw new MarmotFileException("not cluster dir: path=" + clusterDir);
			}

			m_clusterDir = clusterDir;
			m_indexFilePath = GlobalIndex.toGlobalIndexPath(m_clusterDir);
			if ( !m_indexFilePath.exists() || m_indexFilePath.isDirectory() ) {
				throw new MarmotFileException("not index file: path=" + m_indexFilePath);
			}

			m_globalIdx = GlobalIndex.open(m_indexFilePath);
			m_schema = m_globalIdx.getGRecordSchema().getRecordSchema();
		}
		catch ( IOException e ) {
			throw new MarmotFileException("fails to get ClusterIndex: path=" + clusterDir
											+ ", cause=" + e);
		}
	}
	
	public HdfsPath getClusterDir() {
		return m_clusterDir;
	}
	
	public long getBlockSize() {
		try {
			return m_indexFilePath.getFileStatus().getBlockSize();
		}
		catch ( IOException e ) {
			throw new MarmotFileException(e);
		}
	}
	
	public GlobalIndex getGlobalIndex() {
		return m_globalIdx;
	}
	
	public GRecordSchema getGRecordSchema() {
		return m_globalIdx.getGRecordSchema();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	public Envelope getDataBounds() {
		return m_globalIdx.getDataBounds();
	}

	@Override
	public Envelope getQuadBounds() {
		return m_globalIdx.getTileBounds();
	}

	@Override
	public int getClusterCount() {
		return m_globalIdx.getClusterCount();
	}

	@Override
	public long getRecordCount() {
		return m_globalIdx.getRecordCount();
	}

	@Override
	public long getDuplicateCount() {
		return m_globalIdx.getOwnedRecordCount();
	}

	@Override
	public Set<String> getClusterKeyAll() {
		return m_globalIdx.getClusterKeyAll();
	}

	@Override
	public SpatialIndexedCluster getCluster(String quadKey) throws SpatialIndexedFileException {
		Utilities.checkNotNullArgument(quadKey, "quadkey is null");
		
		GlobalIndexEntry cidx = m_globalIdx.get(quadKey);
		if ( cidx == null ) {
			throw new IllegalArgumentException("invalid quadkey: " + quadKey);
		}
		
		return SpatialIndexedCluster.load(m_clusterDir, cidx);
	}

	@Override
	public FStream<String> queryClusterKeys(Envelope range84) {
		return m_globalIdx.query(range84).map(GlobalIndexEntry::quadKey);
	}
	
	public static FStream<Match<GlobalIndexEntry>>
	matchClusters(SpatialIndexedFile left, SpatialIndexedFile right) {
		return GlobalIndex.matchClusters(left.getGlobalIndex(), right.getGlobalIndex());
	}
	
	@Override
	public String toString() {
		return String.format("%s: path=%s",
							SpatialIndexedFile.class.getSimpleName(), m_clusterDir);
	}
}
