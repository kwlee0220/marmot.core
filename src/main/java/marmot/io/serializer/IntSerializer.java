package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;

import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class IntSerializer implements ComparableMarmotSerDe<Integer> {
	@Override
	public DataType getDataType() {
		return DataType.INT;
	}
	
	@Override
	public void serialize(Integer data, DataOutput out) {
		MarmotSerializers.writeInt((int)data, out);
	}

	@Override
	public Integer deserialize(DataInput in) {
		return MarmotSerializers.readInt(in);
	}

	@Override
	public int compareBytes(Cursor cursor) {
		return cursor.compareInt();
	}
}
