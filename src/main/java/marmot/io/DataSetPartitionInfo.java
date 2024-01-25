package marmot.io;

import java.io.Serializable;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.Envelope;

import marmot.Record;
import marmot.RecordSchema;
import marmot.support.DefaultRecord;
import marmot.type.DataType;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class DataSetPartitionInfo implements Serializable {
	private static final long serialVersionUID = 1L;

	public static final DataSetPartitionInfo EMPTY = new DataSetPartitionInfo(new Envelope(), 0L, 0L);
	public static final RecordSchema SCHEMA = RecordSchema.builder()
															.addColumn("bounds", DataType.ENVELOPE)
															.addColumn("count", DataType.LONG)
															.addColumn("size", DataType.LONG)
															.build();
	
	@Nullable private final Envelope m_bounds;
	private final long m_count;
	private final long m_size;
	
	public DataSetPartitionInfo(Envelope bounds, long count, long size) {
		m_bounds = bounds;
		m_count = count;
		m_size = size;
	}
	
	public Envelope bounds() {
		return m_bounds;
	}
	
	public long count() {
		return m_count;
	}
	
	public long size() {
		return m_size;
	}
	
	public static DataSetPartitionInfo from(Record record) {
		return new DataSetPartitionInfo((Envelope)record.get(0), record.getLong(1),
										record.getLong(2));
	}
	
	public static DataSetPartitionInfo from(Object[] values) {
		return new DataSetPartitionInfo((Envelope)values[0], (Long)values[1], (Long)values[2]);
	}
	
	public void copyTo(Record output) {
		output.set(0, m_bounds);
		output.set(1, m_count);
		output.set(2, m_size);
	}
	
	public Record toRecord() {
		Record rec = DefaultRecord.of(SCHEMA);
		copyTo(rec);
		
		return rec;
	}
	
	public DataSetPartitionInfo duplicate() {
		return new DataSetPartitionInfo(new Envelope(m_bounds), m_count, m_size);
	}
	
	@Override
	public String toString() {
		String sizeStr = UnitUtils.toByteSizeString(m_size);
		String boundsStr = (m_bounds != null) ? ", " + m_bounds.toString() : "";
		return String.format("count=%d, size=%s%s", m_count, sizeStr, boundsStr);
	}
	
	public static final DataSetPartitionInfo plus(DataSetPartitionInfo left, DataSetPartitionInfo right) {
		Envelope bounds = new Envelope(left.m_bounds);
		bounds.expandToInclude(right.m_bounds);
		return new DataSetPartitionInfo(bounds, left.m_count+right.m_count, left.m_size+right.m_size);
	}
}
