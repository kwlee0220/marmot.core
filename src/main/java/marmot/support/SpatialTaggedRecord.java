package marmot.support;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Map;

import org.locationtech.jts.geom.Envelope;

import marmot.Record;
import marmot.RecordSchema;
import marmot.io.RecordWritable;
import marmot.io.geo.quadtree.EnvelopedValue;
import marmot.io.serializer.MarmotSerializable;
import marmot.io.serializer.MarmotSerializers;
import marmot.optor.geo.cluster.Constants;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialTaggedRecord implements Record, EnvelopedValue, MarmotSerializable {
	public static final String COL_QUAD_KEY = Constants.COL_QUAD_KEY;
	public static final String COL_MBR = Constants.COL_MBR;
	
	private final Envelope m_mbr;
	private final Record m_record;
	
	public static SpatialTaggedRecord of(Envelope mbr, Record record) {
		return new SpatialTaggedRecord(mbr, record);
	}
	
	public SpatialTaggedRecord(Envelope mbr, Record record) {
		m_mbr = mbr;
		m_record = record;
	}
	
	@Override
	public Envelope getEnvelope() {
		return m_mbr;
	}
	
	public Record getDataRecord() {
		return m_record;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_record.getRecordSchema();
	}

	@Override
	public Object get(int index) {
		return m_record.get(index);
	}

	@Override
	public Object get(String name) {
		return m_record.get(name);
	}

	@Override
	public Object[] getAll() {
		return m_record.getAll();
	}

	@Override
	public Record set(String name, Object value) {
		m_record.set(name, value);
		return this;
	}

	@Override
	public Record set(int idx, Object value) {
		m_record.set(idx, value);
		return this;
	}

	@Override
	public Record set(Record src) {
		m_record.set(src);
		return this;
	}

	@Override
	public Record set(Map<String, Object> values) {
		m_record.set(values);
		return this;
	}

	@Override
	public Record setAll(Iterable<?> values) {
		m_record.setAll(values);
		return this;
	}

	@Override
	public Record setAll(Object... values) {
		m_record.setAll(values);
		return this;
	}

	@Override
	public void clear() {
		m_record.clear();
	}

	@Override
	public Record duplicate() {
		return new SpatialTaggedRecord(m_mbr, m_record.duplicate());
	}
	
	public static SpatialTaggedRecord deserialize(RecordSchema schema, DataInput in) {
		Envelope mbr = MarmotSerializers.ENVELOPE.deserialize(in);
		Record dataRecord = RecordWritable.loadRecord(schema, in);
		
		return new SpatialTaggedRecord(mbr, dataRecord);
	}
	
	@Override
	public void serialize(DataOutput out) {
		MarmotSerializers.ENVELOPE.serialize(m_mbr, out);
		RecordWritable.from(m_record).write(out);
	}
	
	@Override
	public String toString() {
		return String.format("%s:%s", m_mbr, m_record);
	}
}
