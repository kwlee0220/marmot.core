package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.DoubleWritable;

import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DoubleSerializer implements ComparableMarmotSerDe<Double> {
	private static final int BYTE_SIZE = 8;

	@Override
	public DataType getDataType() {
		return DataType.DOUBLE;
	}

	@Override
	public void serialize(Double data, DataOutput out) {
		MarmotSerializers.writeDouble(data, out);
	}

	@Override
	public Double deserialize(DataInput in) {
		return MarmotSerializers.readDouble(in);
	}

	@Override
	public int compareBytes(Cursor cursor) {
		int cmp = DoubleWritable.Comparator.compareBytes(cursor.m_buf1, cursor.m_offset1, BYTE_SIZE,
														cursor.m_buf2, cursor.m_offset2, BYTE_SIZE);
		cursor.m_offset1 += BYTE_SIZE;
		cursor.m_offset2 += BYTE_SIZE;
		
		return cmp;
	}
}
