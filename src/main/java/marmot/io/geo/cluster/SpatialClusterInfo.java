package marmot.io.geo.cluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import org.apache.commons.lang.SerializationException;
import org.locationtech.jts.geom.Envelope;

import marmot.Record;
import marmot.RecordSchema;
import marmot.io.DataSetPartitionInfo;
import marmot.io.serializer.MarmotSerializable;
import marmot.io.serializer.MarmotSerializers;
import marmot.support.DefaultRecord;
import marmot.type.DataType;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class SpatialClusterInfo implements MarmotSerializable, Serializable {
	private static final long serialVersionUID = 1L;

	public static final RecordSchema SCHEMA = RecordSchema.builder()
														.addColumn("quad_key", DataType.STRING)
														.addColumn("quad_bounds", DataType.ENVELOPE)
														.addColumn("data_bounds", DataType.ENVELOPE)
														.addColumn("count", DataType.LONG)
														.addColumn("replica_count", DataType.LONG)
														.addColumn("partition_id", DataType.STRING)
														.addColumn("offset", DataType.LONG)
														.addColumn("length", DataType.LONG)
														.build();
	
	private String m_quadKey;
	private Envelope m_quadBounds;	// EPSG:4326 좌표로 기록
	private Envelope m_dataBounds;
	private long m_count;				// 복사본을 포함한 갯수
	private long m_replicaCount;		// 복사본 갯수
	private String m_partId;
	private long m_start;
	private long m_length;
	
	private SpatialClusterInfo() { }
	public SpatialClusterInfo(String quadKey, Envelope quadBounds, Envelope dataBounds, long count,
								long replicaCount, String partId, long offset, long length) {
		m_quadKey = quadKey;
		m_quadBounds = quadBounds;
		m_dataBounds = dataBounds;
		m_count = count;
		m_replicaCount = replicaCount;
		m_partId = partId;
		m_start = offset;
		m_length = length;
	}
	
	public String quadKey() {
		return m_quadKey;
	}
	
	public Envelope quadBounds() {
		return m_quadBounds;
	}
	
	public Envelope dataBounds() {
		return m_dataBounds;
	}
	
	public long recordCount() {
		return m_count;
	}
	
	public long duplicateCount() {
		return m_replicaCount;
	}
	
	public String partitionId() {
		return m_partId;
	}
	
	public long start() {
		return m_start;
	}
	
	public long length() {
		return m_length;
	}
	
	static SpatialClusterInfo from(Record record) {
		return new SpatialClusterInfo(record.getString(0), (Envelope)record.get(1),
										(Envelope)record.get(2), record.getLong(3),
										record.getLong(4), record.getString(5),
										record.getLong(6), record.getLong(7));
	}
	
	public DataSetPartitionInfo toDataSetPartitionInfo() {
		return new DataSetPartitionInfo(m_dataBounds, m_count-m_replicaCount, m_length);
	}
	
	public Record to(Record output) {
		output.set(0, m_quadKey);
		output.set(1, m_quadBounds);
		output.set(2, m_dataBounds);
		output.set(3, m_count);
		output.set(4, m_replicaCount);
		output.set(5, m_partId);
		output.set(6, m_start);
		output.set(7, m_length);
		
		return output;
	}
	
	public Record toRecord() {
		return to(DefaultRecord.of(SCHEMA));
	}
	
	public static SpatialClusterInfo deserialize(DataInput input) {
		String quadKey = MarmotSerializers.readString(input);
		Envelope quadBounds = MarmotSerializers.ENVELOPE.deserialize(input);
		Envelope dataBounds = MarmotSerializers.ENVELOPE.deserialize(input);
		long count = MarmotSerializers.readLong(input);
		long replicaCount = MarmotSerializers.readLong(input);
		String partId = MarmotSerializers.readString(input);
		long offset = MarmotSerializers.readLong(input);
		long length = MarmotSerializers.readLong(input);
		
		return new SpatialClusterInfo(quadKey, quadBounds, dataBounds, count, replicaCount, partId,
										offset, length);
	}

	@Override
	public void serialize(DataOutput out) {
		MarmotSerializers.writeString(m_quadKey, out);
		MarmotSerializers.ENVELOPE.serialize(m_quadBounds, out);
		MarmotSerializers.ENVELOPE.serialize(m_dataBounds, out);
		MarmotSerializers.writeLong(m_count, out);
		MarmotSerializers.writeLong(m_replicaCount, out);
		MarmotSerializers.writeString(m_partId, out);
		MarmotSerializers.writeLong(m_start, out);
		MarmotSerializers.writeLong(m_length, out);
	}
	
	@Override
	public String toString() {
		return String.format("%s: quadkey=%s, count=%d(%d), %d+%d]", getClass().getSimpleName(),
							m_quadKey, m_count, m_replicaCount, m_start, m_length);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || !(obj instanceof SpatialClusterInfo) ) {
			return false;
		}
		
		SpatialClusterInfo other = (SpatialClusterInfo)obj;
		return m_quadKey.equals(other.m_quadKey);
	}
	
	@Override
	public int hashCode() {
		return m_quadKey.hashCode();
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final byte[] m_bytes;
		
		private SerializationProxy(SpatialClusterInfo scInfo) {
			m_bytes = MarmotSerializers.toBytes(scInfo);
		}
		
		private Object readResolve() {
			try {
				SpatialClusterInfo scInfo = Utilities.callPrivateConstructor(SpatialClusterInfo.class);
				return MarmotSerializers.fromBytes(m_bytes, in -> scInfo.deserialize(in));
			}
			catch (Exception e ) {
				throw new SerializationException(e);
			}
		}
	}
}