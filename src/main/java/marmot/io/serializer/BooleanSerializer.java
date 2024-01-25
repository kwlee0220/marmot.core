package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.BooleanWritable;

import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BooleanSerializer implements ComparableMarmotSerDe<Boolean> {
	private static final int BYTE_SIZE = 1;

	@Override
	public DataType getDataType() {
		return DataType.BOOLEAN;
	}
	
	@Override
	public void serialize(Boolean data, DataOutput out) {
		MarmotSerializers.writeBoolean(data, out);
	}

	@Override
	public Boolean deserialize(DataInput in) {
		return MarmotSerializers.readBoolean(in);
	}

	@Override
	public int compareBytes(Cursor cursor) {
		int cmp = BooleanWritable.Comparator.compareBytes(cursor.m_buf1, cursor.m_offset1, BYTE_SIZE,
														cursor.m_buf2, cursor.m_offset2, BYTE_SIZE);
		cursor.m_offset1 += BYTE_SIZE;
		cursor.m_offset2 += BYTE_SIZE;
		
		return cmp;
	}
}
