package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.ShortWritable;

import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ShortSerializer implements ComparableMarmotSerDe<Short> {
	private static final int BYTE_SIZE = 2;
	
	@Override
	public DataType getDataType() {
		return DataType.SHORT;
	}

	@Override
	public void serialize(Short data, DataOutput out) {
		MarmotSerializers.writeShort(data, out);
	}

	@Override
	public Short deserialize(DataInput in) {
		return MarmotSerializers.readShort(in);
	}

	@Override
	public int compareBytes(Cursor cursor) {
		int cmp = ShortWritable.Comparator.compareBytes(cursor.m_buf1, cursor.m_offset1, BYTE_SIZE,
														cursor.m_buf2, cursor.m_offset2, BYTE_SIZE);
		cursor.m_offset1 += BYTE_SIZE;
		cursor.m_offset2 += BYTE_SIZE;
		
		return cmp;
	}
}
