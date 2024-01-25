package marmot.io.geo.cluster;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.locationtech.jts.geom.Envelope;

import com.google.common.collect.Lists;

import utils.func.FOption;
import utils.io.IOUtils;
import utils.io.Lz4Compressions;
import utils.stream.FStream;
import utils.stream.FStreams.AbstractFStream;

import marmot.GRecordSchema;
import marmot.Record;
import marmot.RecordSchema;
import marmot.io.HdfsPath;
import marmot.io.MarmotFileException;
import marmot.io.RecordWritable;
import marmot.io.geo.quadtree.Pointer;
import marmot.io.geo.quadtree.PointerPartition;
import marmot.io.geo.quadtree.QuadTree;
import marmot.support.EnvelopeTaggedRecord;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialCluster extends QuadTreeSpatialCluster
							implements CacheableQuadCluster, Serializable {
	private static final long serialVersionUID = 1L;
	
	private final HdfsPath m_path;
	private final SpatialClusterInfo m_info;

	private QuadTree<Pointer,PointerPartition> m_qtree;
	private List<EnvelopeTaggedRecord> m_records;
	
	public SpatialCluster(HdfsPath path, SpatialClusterInfo scInfo, GRecordSchema gschema) {
		super(scInfo.quadKey(), gschema);
		
		m_path = path;
		m_info = scInfo;
		
		m_records = Lists.newArrayListWithExpectedSize((int)m_info.recordCount());
		if ( !isOutlier() ) {
			m_qtree = new QuadTree<>(m_info.quadKey(), qkey->new PointerPartition());
		}
		try ( FSDataInputStream fsin = m_path.open() ) {
			if ( m_info.start() > 0 ) {
				fsin.seek(m_info.start());
			}
			
			RecordWritable writable = RecordWritable.from(gschema.getRecordSchema());
			DataInputStream dis = new DataInputStream(Lz4Compressions.decompress(fsin));
			for ( int idx = 0; idx < m_info.recordCount(); ++idx ) {
				writable.readFields(dis);
				
				Record record = writable.toRecord();
				Envelope mbr = (isOutlier()) ? new Envelope() : getMbr84(record);
				m_records.add(new EnvelopeTaggedRecord(mbr, record));
				
				if ( !isOutlier() ) {
					m_qtree.insert(new Pointer(mbr, idx));
				}
			}
		}
		catch ( IOException e ) {
			throw new MarmotFileException("fails to load SpatialCluster: path=" + m_path + ", cause=" + e);
		}
	}
	
	public static FStream<Record> readAll(HdfsPath path, SpatialClusterInfo scInfo, RecordSchema schema) {
		return new RecordStream(path, scInfo.start(), scInfo.recordCount(), schema);
	}
	
	public static FStream<Record> readNonDuplicate(HdfsPath path, SpatialClusterInfo scInfo,
													RecordSchema schema) {
		long count = scInfo.recordCount() - scInfo.duplicateCount();
		return new RecordStream(path, scInfo.start(), count, schema);
	}
	
	public SpatialClusterInfo getInfo() {
		return m_info;
	}

	@Override
	public Envelope getDataBounds() {
		return m_info.dataBounds();
	}

	@Override
	public long getDuplicateCount() {
		return m_info.duplicateCount();
	}

	@Override
	public int length() {
		return (int)m_info.length();
	}

	@Override
	protected QuadTree<Pointer, PointerPartition> getQuadTree() {
		return m_qtree;
	}

	@Override
	protected List<EnvelopeTaggedRecord> getRecordList() {
		return m_records;
	}
	
	private Envelope getMbr84(Record record) {
		return toWgs84(record.getGeometry(m_geomColIdx).getEnvelopeInternal());
	}
	
	private Object writeReplace() {
		return new SerializationProxy(m_path, m_info, getGRecordSchema());
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final HdfsPath m_path;
		private final SpatialClusterInfo m_info;
		private final GRecordSchema m_gschema;
		
		private SerializationProxy(HdfsPath path, SpatialClusterInfo scInfo, GRecordSchema gschema) {
			m_path = path;
			m_info = scInfo;
			m_gschema = gschema;
		}
		
		private Object readResolve() {
			return new SpatialCluster(m_path, m_info, m_gschema);
		}
	}
	
	private static class RecordStream extends AbstractFStream<Record> {
		private final HdfsPath m_path;
		private final DataInputStream m_dis;
		private final RecordWritable m_writable;
		private long m_remains;
		
		RecordStream(HdfsPath path, long start, long count, RecordSchema schema) {
			try {
				m_path = path;
				FSDataInputStream fsin = path.open();
				if ( start > 0 ) {
					fsin.seek(start);
				}

				m_dis = new DataInputStream(Lz4Compressions.decompress(fsin));
				m_writable = RecordWritable.from(schema);
				m_remains = count;
			}
			catch ( IOException e ) {
				throw new MarmotFileException("fails to load SpatialCluster: path=" + path + ", cause=" + e);
			}
		}

		@Override
		protected void closeInGuard() throws Exception {
			IOUtils.close(m_dis);
		}

		@Override
		public FOption<Record> nextInGuard() {
			if ( m_remains <= 0 ) {
				return FOption.empty();
			}

			try {
				m_writable.readFields(m_dis);
				--m_remains;
				
				return FOption.of(m_writable.toRecord());
			}
			catch ( IOException e ) {
				throw new MarmotFileException("fails to load SpatialCluster: path=" + m_path + ", cause=" + e);
			}
		}
		
	}
}
