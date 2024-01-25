package marmot.io;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.io.Writable;
import org.hsqldb.lib.DataOutputStream;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.io.serializer.MarmotSerializer;
import marmot.io.serializer.MarmotSerializers;
import marmot.io.serializer.SerializationException;
import marmot.support.DataUtils;
import marmot.support.DefaultRecord;
import marmot.type.DataType;
import utils.Throwables;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordWritable implements Writable {
	private static final long serialVersionUID = -9192719791884943141L;
	private static final Logger s_logger = LoggerFactory.getLogger(RecordWritable.class);
	
	private transient RecordSchema m_schema;
	@SuppressWarnings("rawtypes")
	private transient MarmotSerializer[] m_serdes;
	private transient Object[] m_colValues;
	
	public static RecordWritable from(RecordSchema schema) {
		return new RecordWritable(schema, new Object[schema.getColumnCount()]);
	}
	
	public static RecordWritable from(RecordSchema schema, Object[] values) {
		return new RecordWritable(schema, values);
	}
	
	public static RecordWritable from(Record record) {
		return new RecordWritable(record.getRecordSchema(), record.getAll());
	}
	
	// Hadoop internal use only (due to Writable)
	public RecordWritable() { }
	
	private RecordWritable(RecordSchema schema, Object[] values) {
		m_schema = schema;
		m_serdes = m_schema.getColumns().stream()
							.map(Column::type)
							.map(MarmotSerializers::getSerializer)
							.toArray(sz -> new MarmotSerializer[sz]);
		m_colValues = values;
	}
	
	private RecordWritable(Object[] values) {
		m_colValues = values;
	}
	
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public Object[] get() {
		return m_colValues;
	}
	
	public Object get(int idx) {
		return m_colValues[idx];
	}
	
	public void set(Object[] values) {
		m_colValues = values;
	}
	
	public void set(int idx, Object value) {
		m_colValues[idx] = value;
	}
	
	public Record toRecord(RecordSchema schema) {
		return DefaultRecord.of(schema).setAll(m_colValues);
	}
	
	public Record toRecord() {
		return DefaultRecord.of(m_schema).setAll(m_colValues);
	}
	
	public void loadFrom(Record record) {
		set(record.getAll());
	}
	
	public void storeTo(Record record) {
		record.setAll(m_colValues);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		short ncols = in.readShort();
		if ( ncols == -1 ) {
			throw new EOFException();
		}
		
		m_colValues = new Object[ncols];
		for ( int i =0; i < ncols; ++i ) {
			int tc = MarmotSerializers.readNullableTypeCode(in);
			if ( tc >= 0 ) {
				MarmotSerializer<?> serde = null;
				if ( m_serdes != null ) {
					serde = m_serdes[i];
				}
				else {
					serde = MarmotSerializers.getSerializer(tc);
				}
				
				m_colValues[i] = serde.deserialize(in);
			}
			else {
				m_colValues[i] = null;
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void write(DataOutput out) {
		try {
			// 맨 처음에 컴럼의 갯수를 기록한다.
			out.writeShort(m_schema.getColumnCount());
			
			int i =0;
			for ( Column col: m_schema.getColumns() ) {
				DataType type = col.type();
				Object value = m_colValues[i];
				
				byte tc = (byte)type.getTypeCode().get();
				if ( value != null ) {
					MarmotSerializers.writeByte(tc, out);
					try {
						value = DataUtils.cast(value, type);
						m_serdes[i].serialize(value, out);
					}
					catch ( Exception e ) {
						Record rec = DefaultRecord.of(m_schema);
						rec.setAll(m_colValues);
						Map<String,Object> vmap = Maps.newHashMap();
						for ( Map.Entry<String,Object> ent: rec.toMap().entrySet() ) {
							if ( !(ent.getValue() instanceof Geometry) ) {
								vmap.put(ent.getKey(), ent.getValue());
							}
						}
						
						s_logger.error("fails to serialize the column: {}:'{}', record={}, cause={}",
										col.name(), value, vmap, ""+e);
						throw e;
					}
				}
				else {
					out.writeByte(-tc);
				}
				
				++i;
			}
		}
		catch ( Exception e ) {
			Throwables.throwIfInstanceOf(e, SerializationException.class);
			throw new SerializationException("" + e);
		}
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		RecordWritable other = (RecordWritable)obj;
		return Arrays.equals(m_colValues, other.m_colValues);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_colValues);
	}
	
	@Override
	public String toString() {
		return Stream.of(m_colValues)
					.map(v -> v != null ? v.toString() : "null")
					.collect(Collectors.joining(",", "[", "]"));
	}
	
	public RecordWritable duplicate() {
		RecordWritable copy = new RecordWritable();
		copy.m_colValues = m_colValues;
		copy.m_schema = m_schema;
		copy.m_serdes = m_serdes;
		
		return copy;
	}
	
	public byte[] toBytes() {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(baos));
			this.write(dos);
			dos.close();
			
			return baos.toByteArray();
		}
		catch ( IOException e ) {
			throw new RecordSetException("fails to deserialize record", e);
		}
	}
	
	public RecordWritable fromBytes(byte[] bytes) {
		try ( DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes)) ) {
			readFields(dis);
			
			return this;
		}
		catch ( IOException e ) {
			throw new RecordSetException("fails to deserialize record", e);
		}
	}
	
	public static void fromBytes(byte[] bytes, Record output) {
		RecordWritable writer = RecordWritable.from(output.getRecordSchema());
		try ( DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes)) ) {
			writer.readFields(dis);
			output.setAll(writer.get());
		}
		catch ( IOException e ) {
			throw new RecordSetException("fails to deserialize record", e);
		}
	}
	
	public static Record fromBytes(byte[] bytes, RecordSchema schema) {
		Record record = DefaultRecord.of(schema);
		fromBytes(bytes, record);
		
		return record;
	}
	
	public static Record loadRecord(RecordSchema schema, DataInput input) {
		try {
			RecordWritable writable = from(schema);
			writable.readFields(input);
			return writable.toRecord();
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}
}
