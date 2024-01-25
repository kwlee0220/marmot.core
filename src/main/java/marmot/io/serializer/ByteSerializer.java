package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.ByteWritable;

import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ByteSerializer implements ComparableMarmotSerDe<Byte> {
	private static final int BYTE_SIZE = 1;
	
	@Override
	public DataType getDataType() {
		return DataType.BYTE;
	}

	@Override
	public void serialize(Byte data, DataOutput out) {
		MarmotSerializers.writeByte(data, out);
	}

	@Override
	public Byte deserialize(DataInput in) {
		return MarmotSerializers.readByte(in);
	}

	@Override
	public int compareBytes(Cursor cursor) {
		int cmp = ByteWritable.Comparator.compareBytes(cursor.m_buf1, cursor.m_offset1, BYTE_SIZE,
														cursor.m_buf2, cursor.m_offset2, BYTE_SIZE);
		cursor.m_offset1 += BYTE_SIZE;
		cursor.m_offset2 += BYTE_SIZE;
		
		return cmp;
	}
}
