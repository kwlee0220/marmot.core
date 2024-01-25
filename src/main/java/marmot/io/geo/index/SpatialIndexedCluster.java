package marmot.io.geo.index;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import marmot.GRecordSchema;
import marmot.io.HdfsPath;
import marmot.io.geo.cluster.CacheableQuadCluster;
import marmot.io.geo.cluster.QuadTreeSpatialCluster;
import marmot.io.geo.quadtree.LeafNode;
import marmot.io.geo.quadtree.Pointer;
import marmot.io.geo.quadtree.PointerPartition;
import marmot.io.geo.quadtree.QuadTree;
import marmot.io.geo.quadtree.QuadTreeBuilder;
import marmot.io.serializer.MarmotSerializable;
import marmot.io.serializer.MarmotSerializers;
import marmot.optor.geo.SpatialRelation;
import marmot.support.EnvelopeTaggedRecord;
import marmot.type.MapTile;
import utils.UnitUtils;
import utils.Utilities;
import utils.io.IOUtils;
import utils.io.Lz4Compressions;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialIndexedCluster extends QuadTreeSpatialCluster
									implements CacheableQuadCluster, MarmotSerializable {
	@SuppressWarnings("unused")
	private static final Logger s_logger = LoggerFactory.getLogger(SpatialIndexedCluster.class);
	
	private final Envelope m_dataBounds;
	private final List<EnvelopeTaggedRecord> m_records;
	private int m_length;
	private final long m_duplicateCount;
	private QuadTree<Pointer,PointerPartition> m_qtree;
	
	public static SpatialIndexedCluster load(HdfsPath clusterDir,
										GlobalIndexEntry index) throws SpatialIndexedFileException {
		return load(clusterDir.child(index.packId()), index.start(), index.length());
	}
	
	/**
	 * 파일에서 FileBasedCluster을  적재한다.
	 * 
	 * @param fs		HDFS 파일시스템
	 * @param path		IndexedCluster가 저장된 파일 경로.
	 * @param start		파일에서 IndexedCluster가 시작되는 offset
	 * @return 적재된 FileBasedCluster 객체.
	 * @throws IOException	파일기반 클러스트 생성 중 오류가 발생된 경우.
	 */
	public static SpatialIndexedCluster load(HdfsPath path, long start, long length)
		throws SpatialIndexedFileException {
		Utilities.checkNotNullArgument(path, "path is null");
		Utilities.checkArgument(start >= 0, "invalid offset: " + start);

		try {
			byte[] partition = new byte[(int)length];
			try ( FSDataInputStream fsdi = path.open() ) {
				fsdi.seek(start);
				IOUtils.readFully(fsdi, partition);
			}
			
			return fromBytes(partition, 0, partition.length);
		}
		catch ( IOException e ) {
			throw new SpatialIndexedFileException("" + e);
		}
	}

	private SpatialIndexedCluster(String quadKey, Envelope dataBounds, GRecordSchema gschema,
									List<EnvelopeTaggedRecord> recordList, long duplicateCount,
									QuadTree<Pointer, PointerPartition> qtree) {
		super(quadKey, gschema);
		Utilities.checkArgument(recordList.size() > 0, "empty records");
		
		m_dataBounds = dataBounds;
		m_records = recordList;
		m_length = -1;
		m_duplicateCount = duplicateCount;
		m_qtree = qtree;
	}

	@Override
	public Envelope getDataBounds() {
		return m_dataBounds;
	}

	@Override
	public long getDuplicateCount() {
		return m_duplicateCount;
	}
	
	public SpatialIndexedCluster length(int length) {
		m_length = length;
		return this;
	}

	@Override
	public int length() {
		return m_length;
	}
	
	@Override
	public long getRecordCount() {
		return m_records.size();
	}

	@Override
	public FStream<EnvelopeTaggedRecord> read(boolean dropDuplicates) {
		FStream<EnvelopeTaggedRecord> strm = FStream.from(m_records);
		if ( dropDuplicates ) {
			strm = strm.filter(etr -> isOwnerOf(etr.getEnvelope()));
		}
		return strm;
	}

	@Override
	public FStream<EnvelopeTaggedRecord> query(Envelope range84, boolean dropDuplicates) {
		FStream<EnvelopeTaggedRecord> strm =  m_qtree.query(SpatialRelation.INTERSECTS, range84)
													.distinct()
													.map(ptr -> m_records.get(ptr.index()));
		if ( dropDuplicates ) {
			strm = strm.filter(etr -> isOwnerOf(etr.getEnvelope().intersection(range84)));
		}
		return strm;
	}
	
	public static SpatialIndexedCluster build(String quadKey, GRecordSchema gschema,
													FStream<EnvelopeTaggedRecord> recs) {
		int geomColIdx = gschema.getGeometryColumnIdx();
		final Envelope tileBounds =  MapTile.fromQuadKey(quadKey).getBounds();
		Envelope dataBounds = new Envelope();
		List<EnvelopeTaggedRecord> recList = Lists.newArrayList();
		QuadTree<Pointer,PointerPartition> qtree = new QuadTree<>(quadKey, qkey->new PointerPartition());
		
		int m_count = 0;
		int m_ownedCount = 0;
		for ( EnvelopeTaggedRecord record: recs ) {
			recList.add(record);
			
			Geometry geom = record.getRecord().getGeometry(geomColIdx);
			dataBounds.expandToInclude(geom.getEnvelopeInternal());
			
			Envelope mbr = record.getEnvelope();
			dataBounds.expandToInclude(mbr);
			if ( tileBounds.contains(mbr.centre()) ) {
				++m_ownedCount;
			}
			
			qtree.insert(new Pointer(mbr, m_count));
			++m_count;
		}
		
		return new SpatialIndexedCluster(quadKey, dataBounds, gschema, recList, m_ownedCount, qtree);
	}
	
	public static SpatialIndexedCluster deserialize(DataInput input) {
		String quadKey = MarmotSerializers.readString(input);
		Envelope dataBounds = MarmotSerializers.ENVELOPE.deserialize(input);
		long duplicateCount = MarmotSerializers.readVLong(input);
		GRecordSchema gschema = MarmotSerializers.readGRecordSchema(input);
		
		List<EnvelopeTaggedRecord> records
				= MarmotSerializers.readList(input,
							in -> EnvelopeTaggedRecord.deserialize(gschema.getRecordSchema(), in));
		QuadTree<Pointer,PointerPartition> qtree = readIndexFrom(input);
		
		return new SpatialIndexedCluster(quadKey, dataBounds, gschema, records,
											duplicateCount, qtree);
	}
	
	@Override
	public void serialize(DataOutput out) {
		MarmotSerializers.writeString(getQuadKey(), out);
		MarmotSerializers.ENVELOPE.serialize(m_dataBounds, out);
		MarmotSerializers.writeVLong(m_duplicateCount, out);
		MarmotSerializers.writeGRecordSchema(getGRecordSchema(), out);
		
		MarmotSerializers.writeList(m_records, out);
		writeIndexInto(out);
	}

	@Override
	protected QuadTree<Pointer, PointerPartition> getQuadTree() {
		return m_qtree;
	}

	@Override
	protected List<EnvelopeTaggedRecord> getRecordList() {
		return m_records;
	}
	
	private static QuadTree<Pointer, PointerPartition> readIndexFrom(DataInput input) {
		String quadKey = MarmotSerializers.readString(input);
		
		QuadTreeBuilder<Pointer,PointerPartition> builder
									= new QuadTreeBuilder<>(quadKey, qkey -> new PointerPartition());
		int nodeCount = MarmotSerializers.readVInt(input);
		for ( int i =0; i < nodeCount; ++i ) {
			String qk = MarmotSerializers.readString(input);
			PointerPartition part = PointerPartition.deserialize(input);
			
			builder.add(qk, part);
		}
		QuadTree<Pointer, PointerPartition> qtree = builder.build();
		qtree.compact();
		
		return qtree;
	}

	private void writeIndexInto(DataOutput out) {
		MarmotSerializers.writeString(getQuadKey(), out);
		List<LeafNode<Pointer,PointerPartition>> nodes = m_qtree.streamLeafNodes().toList();
		
		MarmotSerializers.writeVInt(nodes.size(), out);
		for ( LeafNode<Pointer,PointerPartition> node: nodes ) {
			MarmotSerializers.writeString(node.getQuadKey(), out);
			node.getPartition().serialize(out);
		}
	}

	private static final int LZ4_BLOCK_SIZE = (int)UnitUtils.parseByteSize("1mb");
	public static SpatialIndexedCluster fromBytes(byte[] bytes, int offset, int length) {
		try ( ByteArrayInputStream bais = new ByteArrayInputStream(bytes, offset, length);
				InputStream restored = Lz4Compressions.toDecompressedStream(bais);
				DataInputStream input = new DataInputStream(restored) ) {
			return deserialize(input).length(length);
		}
		catch ( IOException e ) {
			throw new AssertionError(e);
		}
	}
	
	public byte[] toBytes(int blockSize) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(blockSize);
		try ( OutputStream compressed = Lz4Compressions.toCompressedStream(baos, LZ4_BLOCK_SIZE);
				DataOutputStream output = new DataOutputStream(compressed) ) {
			serialize(output);
		}
		catch ( IOException e ) {
			throw new AssertionError(e);
		}
		return baos.toByteArray();
	}
	
	@Override
	public String toString() {
		return String.format("%s(key=%s,count=%d)", getClass().getSimpleName(), getQuadKey(),
													m_records.size());
	}
}
