package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.FloatWritable;

import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FloatSerializer implements ComparableMarmotSerDe<Float> {
	private static final int BYTE_SIZE = 4;
	
	@Override
	public DataType getDataType() {
		return DataType.FLOAT;
	}

	@Override
	public void serialize(Float data, DataOutput out) {
		MarmotSerializers.writeFloat(data, out);
	}

	@Override
	public Float deserialize(DataInput in) {
		return MarmotSerializers.readFloat(in);
	}

	@Override
	public int compareBytes(Cursor cursor) {
		int cmp = FloatWritable.Comparator.compareBytes(cursor.m_buf1, cursor.m_offset1, BYTE_SIZE,
														cursor.m_buf2, cursor.m_offset2, BYTE_SIZE);
		cursor.m_offset1 += BYTE_SIZE;
		cursor.m_offset2 += BYTE_SIZE;
		
		return cmp;
	}
}
