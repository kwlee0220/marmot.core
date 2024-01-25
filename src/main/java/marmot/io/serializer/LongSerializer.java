package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;

import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LongSerializer implements ComparableMarmotSerDe<Long> {
	@Override
	public DataType getDataType() {
		return DataType.LONG;
	}
	
	@Override
	public void serialize(Long data, DataOutput out) {
		MarmotSerializers.writeLong(data, out);
	}

	@Override
	public Long deserialize(DataInput in) {
		return MarmotSerializers.readLong(in);
	}

	@Override
	public int compareBytes(Cursor cursor) {
		return cursor.compareLong();
	}
}
