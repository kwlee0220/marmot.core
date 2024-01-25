package marmot.io.geo.cluster;

import static utils.UnitUtils.parseByteSize;
import static utils.UnitUtils.toByteSizeString;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import marmot.GRecordSchema;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.CoordinateTransform;
import marmot.io.HdfsPath;
import marmot.io.MarmotFileException;
import marmot.io.MarmotSequenceFile;
import marmot.io.MarmotSequenceFile.FileInfo;
import marmot.io.RecordWritable;
import marmot.support.DefaultRecord;
import marmot.type.MapTile;
import utils.Utilities;
import utils.io.IOUtils;
import utils.io.IOUtils.CopyStream;
import utils.io.Lz4Compressions;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialClusterFile implements QuadClusterFile<SpatialCluster>, Serializable {
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unused")
	private static final Logger s_logger = LoggerFactory.getLogger(SpatialClusterFile.class);
	
	public static final String CLUSTER_INDEX_FILE = "cluster.idx";
	public static final String PROP_DATASET_SCHEMA = MarmotSequenceFile.MARMOT_FILE_KEY_PREFIX + "dataset.schema";
	public static final String PROP_GEOM_COL = MarmotSequenceFile.MARMOT_FILE_KEY_PREFIX + "dataset.geom_column";
	public static final String PROP_SRID = MarmotSequenceFile.MARMOT_FILE_KEY_PREFIX + "dataset.geom.srid";
	
	private final HdfsPath m_path;
	private final GRecordSchema m_gschema;
	private final Map<String,SpatialClusterInfo> m_scInfos;
	private final Envelope m_dataBounds;
	private final Envelope m_quadBounds;
	private final long m_recordCount;
	private final long m_replicaCount;
	
	public static SpatialClusterFile of(HdfsPath path) {
		return new SpatialClusterFile(path);
	}
	
	private SpatialClusterFile(HdfsPath path) {
		Utilities.checkNotNullArgument(path, "path is null");
		
		m_path = path;
		
		MarmotSequenceFile idxFile = MarmotSequenceFile.of(path.child(CLUSTER_INDEX_FILE));
		FileInfo info = idxFile.getFileInfo();
		Map<String,String> metadata = info.getMetadata();
		
		String geomCol = metadata.get(PROP_GEOM_COL);
		String srid = metadata.get(PROP_SRID);
		GeometryColumnInfo gcInfo = new GeometryColumnInfo(geomCol, srid);
		RecordSchema schema = RecordSchema.parse(metadata.get(PROP_DATASET_SCHEMA));
		m_gschema = new GRecordSchema(gcInfo, schema);
		
		m_scInfos = idxFile.read().fstream()
							.map(rec -> SpatialClusterInfo.from(rec))
							.toMap(SpatialClusterInfo::quadKey, Function.identity());
		
		m_quadBounds = new Envelope();
		m_dataBounds = new Envelope();
		long nrecords =0;
		long nreplicas =0;
		for ( SpatialClusterInfo scInfo: m_scInfos.values() ) {
			if ( !scInfo.quadKey().equals("outliers") ) {
				m_quadBounds.expandToInclude(scInfo.quadBounds());
				m_dataBounds.expandToInclude(scInfo.dataBounds());
			}
			nrecords += scInfo.recordCount();
			nreplicas += scInfo.duplicateCount();
		}
		m_recordCount = nrecords;
		m_replicaCount = nreplicas;
	}
	
	/**
	 * 공간 클러스터 데이터세트의 저장 경로명을 반환한다.
	 * 
	 * @return	저장 경로명
	 */
	public HdfsPath getPath() {
		return m_path;
	}
	
	/**
	 * 공간 클러스터 데이터세트에 저장된 레코드들의 스키마를 반환한다.
	 * 
	 * @return	레코드 스키마
	 */
	public GRecordSchema getGRecordSchema() {
		return m_gschema;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_gschema.getRecordSchema();
	}

	@Override
	public Envelope getDataBounds() {
		return m_dataBounds;
	}

	@Override
	public Envelope getQuadBounds() {
		return m_quadBounds;
	}
	
	public long getRecordCount() {
		return m_recordCount;
	}
	
	public long getDuplicateCount() {
		return m_replicaCount;
	}

	@Override
	public int getClusterCount() {
		return m_scInfos.size();
	}

	@Override
	public SpatialCluster getCluster(String quadKey) {
		Utilities.checkNotNullArgument(quadKey, "quadKey is null");
		
		SpatialClusterInfo scInfo = m_scInfos.get(quadKey);
		if ( scInfo != null ) {
			return toSpatialCluster(scInfo);
		}
		else {
			throw new IllegalArgumentException("invalid quadkey: " + quadKey);
		}
	}

	public SpatialClusterInfo getClusterInfo(String quadKey) {
		Utilities.checkNotNullArgument(quadKey, "quadKey is null");
		
		SpatialClusterInfo scInfo = m_scInfos.get(quadKey);
		if ( scInfo != null ) {
			return scInfo;
		}
		else {
			throw new IllegalArgumentException("invalid quadkey: " + quadKey);
		}
	}
	
	private SpatialCluster toSpatialCluster(SpatialClusterInfo scInfo) {
		return new SpatialCluster(m_path.child(scInfo.partitionId()), scInfo, m_gschema);
	}

	@Override
	public FStream<String> queryClusterKeys(Envelope range84) {
		Utilities.checkNotNullArgument(range84, "range is null");
		
		return FStream.from(m_scInfos.values())
						.filter(scInfo -> scInfo.quadBounds().intersects(range84))
						.map(SpatialClusterInfo::quadKey);
	}

	public FStream<SpatialClusterInfo> queryClusterInfos(Envelope range84) {
		Utilities.checkNotNullArgument(range84, "range is null");
		
		return FStream.from(m_scInfos.values())
						.filter(scInfo -> scInfo.quadBounds().intersects(range84));
	}

	@Override
	public Set<String> getClusterKeyAll() {
		return m_scInfos.keySet();
	}

	public FStream<SpatialClusterInfo> getClusterInfoAll() {
		return FStream.from(m_scInfos.values());
	}

	@Override
	public FStream<Record> read() {
		return FStream.from(m_scInfos.values())
						.flatMap(info -> {
							HdfsPath path = m_path.child(info.partitionId());
							return SpatialCluster.readAll(path, info, m_gschema.getRecordSchema())
												.take(info.recordCount() - info.duplicateCount());
						});
	}
	
	public FStream<Record> queryRecord(Envelope range84, Envelope range) {
		return queryClusterKeys(range84)
				.map(qk -> toSpatialCluster(m_scInfos.get(qk)))
				.flatMap(cluster -> cluster.queryRecord(range, true));
	}

	private static int COMPRESS_BUF_SIZE = (int)parseByteSize("4mb");
	public static SpatialClusterInfo storeCluster(HdfsPath path, String quadKey,
													GeometryColumnInfo gcInfo, RecordSet rset,
													long blockSize) {
		Envelope quadBounds = (quadKey.equals("outliers"))
							? null : MapTile.fromQuadKey(quadKey).getBounds();
		CoordinateTransform trans = CoordinateTransform.getTransformToWgs84(gcInfo.srid());
		int geomColIdx = rset.getRecordSchema().getColumn(gcInfo.name()).ordinal();
		Envelope mbr = new Envelope();
		long count = 0;
		long duplicateCount = 0;
		
		CopyStream copy = null;
		PipedInputStream pipeIn = new PipedInputStream(COMPRESS_BUF_SIZE);
		
		try ( FSDataOutputStream fsdos = path.exists() ? path.append() : path.create(true, blockSize);
				PipedOutputStream pipeOut = new PipedOutputStream(pipeIn); ) {
			long start = fsdos.getPos();

			DataOutputStream dos = new DataOutputStream(pipeOut);
			copy = IOUtils.copy(Lz4Compressions.compress(pipeIn), fsdos)
							.closeInputStreamOnFinished(true);
			copy.start();
			
			List<Record> duplicateds = Lists.newArrayList();
			
			Record record = DefaultRecord.of(rset.getRecordSchema());
			while ( rset.next(record) ) {
				boolean isDuplicate = false;
				
				if ( quadBounds != null ) {
					Geometry geom = record.getGeometry(geomColIdx);
					if ( geom != null && !geom.isEmpty() ) {
						Envelope envl = geom.getEnvelopeInternal();
						mbr.expandToInclude(envl);
						
						Coordinate center = envl.centre();
						if ( trans != null ) {
							center = trans.transform(envl.centre());
						}
						if ( !quadBounds.contains(center) ) {
							++duplicateCount;
							isDuplicate = true;
						}
					}
				}
				
				if ( !isDuplicate ) {
					RecordWritable.from(record).write(dos);
				}
				else {
					duplicateds.add(record.duplicate());
				}
				++count;
			}
			
			// duplicate record들을 마지막에 따로 저장함
			FStream.from(duplicateds).forEach(rec -> RecordWritable.from(rec).write(dos));
			IOUtils.closeQuietly(dos);	//  이 연산으로 인해 'copy' 비동기 연산이 완료됨
			
			try {
				copy.get(3, TimeUnit.SECONDS);
			}
			catch ( Exception e ) { }
			
			long size = fsdos.getPos() - start;
			s_logger.info("store_cluster: quadkey={}, count={}+{}, start={}, size={}",
							quadKey, count-duplicateCount, duplicateCount,
							toByteSizeString(start), toByteSizeString(size));
			
			String partId = path.getName();
			return new SpatialClusterInfo(quadKey, quadBounds, mbr, count, duplicateCount, partId,
											start, size);
		}
		catch ( IOException e ) {
			throw new MarmotFileException("fails to write SpatialCluster: quadKey=" + quadKey
											+ ", path=" + path + ", cause=" + e);
		}
		finally {
			if ( copy != null ) {
				copy.cancel(true);
			}
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s: path=%s", getClass().getSimpleName(), m_path);
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;

		private final HdfsPath m_path;
		
		private SerializationProxy(SpatialClusterFile scFile) {
			m_path = scFile.m_path;
		}
		
		private Object readResolve() {
			return SpatialClusterFile.of(m_path);
		}
	}
}
