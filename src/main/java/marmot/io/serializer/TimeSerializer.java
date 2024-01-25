package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.time.LocalTime;

import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TimeSerializer implements ComparableMarmotSerDe<LocalTime> {
	@Override
	public DataType getDataType() {
		return DataType.TIME;
	}
	
	@Override
	public void serialize(LocalTime obj, DataOutput out) {
		long millis = ((LocalTime)obj).toNanoOfDay();
		MarmotSerializers.writeLong(millis, out);
	}

	@Override
	public LocalTime deserialize(DataInput in) {
		return LocalTime.ofNanoOfDay(MarmotSerializers.readLong(in));
	}

	@Override
	public int compareBytes(Cursor cursor) {
		return cursor.compareLong();
	}
}
