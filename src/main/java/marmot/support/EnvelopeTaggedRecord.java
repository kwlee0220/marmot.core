package marmot.support;

import java.io.DataInput;
import java.io.DataOutput;

import org.locationtech.jts.geom.Envelope;

import marmot.Record;
import marmot.RecordSchema;
import marmot.io.RecordWritable;
import marmot.io.geo.quadtree.EnvelopedValue;
import marmot.io.serializer.MarmotSerializable;
import marmot.io.serializer.MarmotSerializers;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class EnvelopeTaggedRecord implements EnvelopedValue, MarmotSerializable {
	private final Envelope m_envl;
	private final Record m_record;
	
	public EnvelopeTaggedRecord(Envelope envl, Record record) {
		m_envl = envl;
		m_record = record;
	}
	
	public Envelope getEnvelope() {
		return m_envl;
	}
	
	public Record getRecord() {
		return m_record;
	}

//	@Override
//	public RecordSchema getRecordSchema() {
//		return m_record.getRecordSchema();
//	}
//
//	@Override
//	public Object get(int index) {
//		return m_record.get(index);
//	}
//
//	@Override
//	public Object get(String name) throws ColumnNotFoundException {
//		return m_record.get(name);
//	}
//
//	@Override
//	public Object[] getAll() {
//		return m_record.getAll();
//	}
//
//	@Override
//	public Record set(String name, Object value) throws ColumnNotFoundException {
//		return m_record.set(name, value);
//	}
//
//	@Override
//	public Record set(int idx, Object value) {
//		return m_record.set(idx, value);
//	}
//
//	@Override
//	public Record set(Record src) {
//		return m_record.set(src);
//	}
//
//	@Override
//	public Record set(Map<String, Object> values) {
//		return m_record.set(values);
//	}
//
//	@Override
//	public Record setAll(Iterable<?> values) {
//		return m_record.setAll(values);
//	}
//
//	@Override
//	public void clear() {
//		m_record.clear();
//	}
//
//	@Override
//	public Record duplicate() {
//		return m_record.duplicate();
//	}
	
	@Override
	public String toString() {
		return String.format("%s:%s", m_envl, m_record);
	}
	
	public static EnvelopeTaggedRecord deserialize(RecordSchema schema, DataInput in) {
		Envelope mbr = MarmotSerializers.ENVELOPE.deserialize(in);
		Record dataRecord = RecordWritable.loadRecord(schema, in);
		
		return new EnvelopeTaggedRecord(mbr, dataRecord);
	}
	
	@Override
	public void serialize(DataOutput out) {
		MarmotSerializers.ENVELOPE.serialize(m_envl, out);
		RecordWritable.from(m_record).write(out);
	}
}
