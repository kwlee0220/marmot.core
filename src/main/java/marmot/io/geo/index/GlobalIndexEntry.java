package marmot.io.geo.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Objects;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.locationtech.jts.geom.Envelope;

import marmot.Record;
import marmot.RecordSchema;
import marmot.io.geo.quadtree.EnvelopedValue;
import marmot.io.serializer.MarmotSerializable;
import marmot.io.serializer.MarmotSerializers;
import marmot.type.DataType;
import marmot.type.MapTile;
import utils.UnitUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GlobalIndexEntry implements EnvelopedValue, MarmotSerializable, Serializable,
												Comparable<GlobalIndexEntry> {
	private static final long serialVersionUID = -5402583185808566922L;
	public static final RecordSchema SCHEMA = RecordSchema.builder()
														.addColumn("pack_id", DataType.STRING)
														.addColumn("block_no", DataType.INT)
														.addColumn("quad_key", DataType.STRING)
														.addColumn("tile_bounds", DataType.ENVELOPE)
														.addColumn("data_bounds", DataType.ENVELOPE)
														.addColumn("count", DataType.INT)
														.addColumn("owned_count", DataType.INT)
														.addColumn("start", DataType.LONG)
														.addColumn("length", DataType.LONG)
														.build();
	private String m_packId = "";
	private int m_blockNo;
	private String m_quadKey;
	private Envelope m_tileBounds;
	private Envelope m_dataBounds;
	private int m_count;
	private int m_ownedCount;
	private long m_start;
	private long m_length;
	
	public GlobalIndexEntry() {}
	public GlobalIndexEntry(String packId, int blockNo, String quadKey,
									Envelope dataBounds, int count, int ownedCount,
									long start, long length) {
		m_packId = packId;
		m_blockNo = blockNo;
		m_quadKey = quadKey;
		m_tileBounds = MapTile.fromQuadKey(m_quadKey).getBounds();
		m_dataBounds = dataBounds;
		m_count = count;
		m_ownedCount = ownedCount;
		m_start = start;
		m_length = length;
	}

	@Override
	public Envelope getEnvelope() {
		return m_tileBounds;
	}
	
	public String packId() {
		return m_packId;
	}
	
	public void packId(String id) {
		m_packId = id;
	}
	
	public int blockNo() {
		return m_blockNo;
	}
	
	public void blockNo(int no) {
		m_blockNo = no;
	}
	
	public String quadKey() {
		return m_quadKey;
	}
	
	public Envelope getTileBounds() {
		return m_tileBounds;
	}
	
	public Envelope getDataBounds() {
		return m_dataBounds;
	}
	
	void setDataBounds(Envelope envl) {
		m_dataBounds = envl;
	}
	
	public int getRecordCount() {
		return m_count;
	}
	
	public int getOwnedRecordCount() {
		return m_ownedCount;
	}
	
	public long start() {
		return m_start;
	}
	
	public void start(long o) {
		m_start = o;
	}
	
	public long length() {
		return m_length;
	}
	
	public boolean isNull() {
		return m_packId.length() == 0;
	}
	
	public static GlobalIndexEntry from(Record record) {
		GlobalIndexEntry cindex = new GlobalIndexEntry();
		cindex.m_packId = record.getString(0);
		cindex.m_blockNo = record.getInt(1);
		cindex.m_quadKey = record.getString(2);
		cindex.m_tileBounds = (Envelope)record.get(3);
		cindex.m_dataBounds = (Envelope)record.get(4);
		cindex.m_count = record.getInt(5);
		cindex.m_ownedCount = record.getInt(6);
		cindex.m_start = record.getLong(7);
		cindex.m_length = record.getLong(8);
		
		return cindex;
	}
	
	public void copyTo(Record record) {
		record.set(0, m_packId);
		record.set(1, m_blockNo);
		record.set(2, m_quadKey);
		record.set(3, m_tileBounds);
		record.set(4, m_dataBounds);
		record.set(5, m_count);
		record.set(6, m_ownedCount);
		record.set(7, m_start);
		record.set(8, m_length);
	}
	
	public static GlobalIndexEntry deserialize(DataInput in) {
		GlobalIndexEntry entry = new GlobalIndexEntry();
		entry.loadFrom(in);
		
		return entry;
	}
	
	@Override
	public void serialize(DataOutput out) {
		MarmotSerializers.writeString(m_packId, out);
		if ( m_packId.length() != 0 ) {
			MarmotSerializers.writeVInt(m_blockNo, out);
			MarmotSerializers.writeString(m_quadKey, out);
			MarmotSerializers.ENVELOPE.serialize(m_dataBounds, out);
			MarmotSerializers.writeInt(m_count, out);
			MarmotSerializers.writeInt(m_ownedCount, out);
			MarmotSerializers.writeLong(m_start, out);
			MarmotSerializers.writeVLong(m_length, out);
		}
	}

	@Override
	public int compareTo(GlobalIndexEntry o) {
		return new CompareToBuilder()
					.append(m_quadKey, o.m_quadKey)
					.append(m_packId, o.m_packId)
					.append(m_start, o.m_start)
					.toComparison();
	}
	
	@Override
	public boolean equals(Object o) {
		if ( this == o ) {
			return true;
		}
		else if ( o == null || !(o instanceof GlobalIndexEntry) ) {
			return false;
		}
		
		GlobalIndexEntry other = (GlobalIndexEntry)o;
		
		return Objects.equals(m_quadKey, other.m_quadKey)
				&& Objects.equals(m_packId, other.m_packId)
				&& Objects.equals(m_start, other.m_start);
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder()
					.append(m_quadKey)
					.append(m_packId)
					.append(m_start)
					.toHashCode();
	}
	
	@Override
	public String toString() {
		return String.format("pack=%s(%d),key=%s,count=%d(%d),start=%d,length=%s",
								m_packId, m_blockNo, m_quadKey, m_count, m_ownedCount,
								m_start, UnitUtils.toByteSizeString(m_length));
	}
	
	private void readObject(ObjectInputStream ois) throws IOException {
		loadFrom(ois);
	}
	
	private void writeObject(ObjectOutputStream oos) throws IOException {
		serialize(oos);
	}
	
	private void loadFrom(DataInput in) {
		m_packId = MarmotSerializers.readString(in);
		if ( m_packId.length() > 0 ) {
			m_blockNo = MarmotSerializers.readVInt(in);
			m_quadKey = MarmotSerializers.readString(in);
			m_tileBounds = MapTile.fromQuadKey(m_quadKey).getBounds();
			m_dataBounds = MarmotSerializers.ENVELOPE.deserialize(in);
			m_count = MarmotSerializers.readInt(in);
			m_ownedCount = MarmotSerializers.readInt(in);
			m_start = MarmotSerializers.readLong(in);
			m_length = MarmotSerializers.readVLong(in);
		}
	}
}