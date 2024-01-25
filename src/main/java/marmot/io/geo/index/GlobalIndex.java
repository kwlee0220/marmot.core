package marmot.io.geo.index;

import static utils.Utilities.checkNotNullArgument;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.GRecordSchema;
import marmot.io.HdfsPath;
import marmot.io.MarmotFileException;
import marmot.io.MarmotSequenceFile;
import marmot.io.geo.quadtree.Partition;
import marmot.io.geo.quadtree.Pointer;
import marmot.io.geo.quadtree.QuadTree;
import marmot.io.geo.quadtree.QuadTreeBuilder;
import marmot.io.geo.quadtree.QuadTreeJoinMatcher;
import marmot.io.serializer.MarmotSerializable;
import marmot.io.serializer.MarmotSerializers;
import marmot.optor.geo.SpatialRelation;
import marmot.optor.support.Match;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GlobalIndex implements MarmotSerializable, Serializable {
	@SuppressWarnings("unused")
	private static final Logger s_logger = LoggerFactory.getLogger(GlobalIndex.class);
	
	private static final String INDEX_FILE_NAME = "cluster.idx";
	public static final String PROP_KEY_CLUSTER_SCHEMA
							= MarmotSequenceFile.MARMOT_FILE_KEY_PREFIX + "cluster_schema";
	
	private String m_path;
	private GRecordSchema m_gschema;
	private List<GlobalIndexEntry> m_indexes;
	private Map<String,GlobalIndexEntry> m_indexMap;
	private QuadTree<Pointer,SinglePointers> m_qtree;
	
	public static Path toGlobalIndexPath(Path clusterDir) {
		return new Path(clusterDir, INDEX_FILE_NAME);
	}
	
	public static HdfsPath toGlobalIndexPath(HdfsPath clusterDir) {
		return clusterDir.child(INDEX_FILE_NAME);
	}
	
	/**
	 * 클러스터 인덱스 파일 생성기를 생성한다.
	 * 
	 * @param path			생성될 클러스트 인덱스 파일 경로.
	 * @param dataSchema	클러스터에 저장될 레코드의 스키마.
	 * @param indexes		파일에 저장될 클러스터 인덱스 리스트
	 * @return	클러스터 인덱스 파일
	 * @throws IOException 
	 */
	public static GlobalIndex create(HdfsPath path, GRecordSchema dataSchema,
												List<GlobalIndexEntry> indexes) {
		try ( FSDataOutputStream fsdos = path.create() ) {
			GlobalIndex idxFile = new GlobalIndex(path.toString(), dataSchema, indexes);
			idxFile.serialize(fsdos);
			return idxFile;
		}
		catch ( IOException e ) {
			String msg = String.format("fails to create %s file: path=%s, cause=%s",
										GlobalIndex.class.getSimpleName(), path, e);
			throw new MarmotFileException(msg);
		}
	}
	
	/**
	 * HDFS에서 클러스터 인덱스 파일을 읽어 클러스터 인덱스 파일 객체를 생성한다.
	 * 
	 *  @param conf	Hadoop 접근을 위한 설정 객체.
	 *  @param path	클러스트 인덱스 파일 경로.
	 *  @return	클러스터 인덱스 파일 객체
	 *  @throws MarmotFileException	파일 객체 생성 중 오류가 발생된 경우.
	 */
	public static GlobalIndex open(HdfsPath path) {
		try {
			if ( path.isDirectory() ) {
				path = path.walkRegularFileTree().findFirst().getOrNull();
				if ( path == null ) {
					throw new MarmotFileException("cannot find index file: path=" + path);
				}
			}
			
			try ( FSDataInputStream fsdis = path.open() ) {
				return deserialize(fsdis);
			}
		}
		catch ( IOException e ) {
			String msg = String.format("fails to read %s file: path=%s, cause=%s",
										GlobalIndex.class.getSimpleName(), path, e);
			throw new MarmotFileException(msg);
		}
	}

	private GlobalIndex(String path, GRecordSchema gschema, List<GlobalIndexEntry> indexes) {
		m_path = path;
		m_gschema = gschema;
		m_indexes = indexes;
		m_indexMap = indexes.stream()
							.collect(Collectors.toMap(GlobalIndexEntry::quadKey, cidx->cidx));
		m_qtree = buildQuadTree(indexes);
	}
	
	/**
	 * 공간 클러스터 인덱스 파일의 저장 경로명을 반환한다.
	 * 
	 * @return	저장 경로명
	 */
	public String getPath() {
		return m_path;
	}
	
	/**
	 * 공간 클러스터 인덱스 파일에 저장된 레코드들의 스키마를 반환한다.
	 * 
	 * @return	레코드 스키마
	 */
	public GRecordSchema getGRecordSchema() {
		return m_gschema;
	}
	
	/**
	 * 식별자에 해당하는 공간 클러스 인덱스를 반환한다.
	 * 
	 * @param	인덱스 식별자
	 * @return	공간 클러스 인덱스
	 */
	public GlobalIndexEntry get(String quadKey) {
		checkNotNullArgument(quadKey);
		
		return m_indexMap.get(quadKey);
	}
	
	/**
	 * 인덱스에 기록된 모든 데이터를 포함하는 MBR을 반환한다.
	 * 
	 * @return	MBR
	 */
	public Envelope getDataBounds() {
		return m_qtree.getDataBounds();
	}
	
	public Envelope getTileBounds() {
		return m_qtree.getBounds();
	}
	
	public int getClusterCount() {
		return m_indexes.size();
	}
	
	public Set<String> getClusterKeyAll() {
		return m_indexMap.keySet();
	}
	
	public long getRecordCount() {
		return FStream.from(m_indexes)
					.mapToLong(ent -> (long)ent.getRecordCount())
					.sum();
	}
	
	public long getOwnedRecordCount() {
		return FStream.from(m_indexes)
						.mapToLong(ent -> (long)ent.getOwnedRecordCount())
						.sum();
	}
	
	public List<GlobalIndexEntry> getIndexEntryAll() {
		return Collections.unmodifiableList(m_indexes);
	}
	
	public FStream<GlobalIndexEntry> query(Envelope range84) {
		return m_qtree.query(SpatialRelation.INTERSECTS, range84)
						.distinct()
						.map(ptr->m_indexes.get(ptr.index()));
	}
	
	@Override
	public String toString() {
		return String.format("%s[key=%s]", getClass().getSimpleName(), m_qtree.getQuadKey());
	}
	
	public static FStream<Match<GlobalIndexEntry>> matchClusters(GlobalIndex left,
																	GlobalIndex right) {
		QuadTreeJoinMatcher<Pointer,SinglePointers> matcher
						= new QuadTreeJoinMatcher<Pointer,SinglePointers>(left.m_qtree, right.m_qtree);
		
		return matcher.streamLeafNodeMatch()
						.map(nodeMatch -> {
							int leftIdx = nodeMatch.m_left.getPartition().m_ptr.index();
							int rightIdx = nodeMatch.m_right.getPartition().m_ptr.index();
							
							GlobalIndexEntry leftEntry = left.m_indexes.get(leftIdx);
							GlobalIndexEntry rightEntry = right.m_indexes.get(rightIdx);
							
							return new Match<GlobalIndexEntry>(leftEntry, rightEntry);
						})
						.filter(match -> {
							Envelope boundsL = match.m_left.getDataBounds();
							Envelope boundsR = match.m_right.getDataBounds();
							return boundsL.intersects(boundsR);
						});
	}
	
	private static QuadTree<Pointer, SinglePointers> buildQuadTree(List<GlobalIndexEntry> indexes) {
		QuadTreeBuilder<Pointer,SinglePointers> builder
													= new QuadTreeBuilder<>(qkey->new SinglePointers());
		for ( int i =0; i < indexes.size(); ++i ) {
			GlobalIndexEntry cidx = indexes.get(i);
			
			Pointer ptr = new Pointer(cidx.getTileBounds(), i);
			builder.add(cidx.quadKey(), new SinglePointers(ptr));
		}
		return builder.build();
	}
	
	public static class SinglePointers implements Partition<Pointer> {
		private Pointer m_ptr = null;
		
		SinglePointers() { }
		
		SinglePointers(Pointer ptr) {
			m_ptr = ptr;
		}
		
		public Pointer getPointer() {
			return m_ptr;
		}

		@Override
		public int size() {
			return (m_ptr != null) ? 1 :0;
		}

		@Override
		public Envelope getBounds() {
			return (m_ptr != null) ? m_ptr.getEnvelope() : new Envelope();
		}
		
		@Override
		public boolean add(Pointer value) {
			if ( m_ptr == null ) {
				m_ptr = value;
				return true;
			}
			else {
				return false;
			}
		}

		@Override
		public FStream<Pointer> values() {
			return (m_ptr != null) ? FStream.of(m_ptr) : FStream.empty();
		}
		
		@Override
		public String toString() {
			return String.format("%s", m_ptr != null ? ""+m_ptr.index() : "null");
		}
	}
	
	public static GlobalIndex deserialize(DataInput in) {
		String path = MarmotSerializers.readString(in);
		GRecordSchema gschema = MarmotSerializers.readGRecordSchema(in);
		List<GlobalIndexEntry> idxList = MarmotSerializers.readList(in, GlobalIndexEntry::deserialize);
		
		return new GlobalIndex(path, gschema, idxList);
	}
	
	@Override
	public void serialize(DataOutput out) {
		MarmotSerializers.writeString(m_path, out);
		MarmotSerializers.writeGRecordSchema(m_gschema, out);
		MarmotSerializers.writeList(m_indexes, out, GlobalIndexEntry::serialize);
	}
	
	private void readObject(ObjectInputStream in) throws IOException {
		m_path = MarmotSerializers.readString(in);
		m_gschema = MarmotSerializers.readGRecordSchema(in);
		
		m_indexes = MarmotSerializers.readList(in, GlobalIndexEntry::deserialize);
		m_indexMap = m_indexes.stream().collect(Collectors.toMap(GlobalIndexEntry::quadKey,
																	cidx->cidx));
		m_qtree = buildQuadTree(m_indexes);
	}
	
	private void writeObject(ObjectOutputStream oos) throws IOException {
		serialize(oos);
	}
}
