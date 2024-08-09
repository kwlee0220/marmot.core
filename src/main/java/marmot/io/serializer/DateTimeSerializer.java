package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.time.LocalDateTime;

import utils.LocalDateTimes;

import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DateTimeSerializer implements ComparableMarmotSerDe<LocalDateTime> {
	@Override
	public DataType getDataType() {
		return DataType.DATETIME;
	}
	
	@Override
	public void serialize(LocalDateTime obj, DataOutput out) {
		long millis = LocalDateTimes.toUtcMillis(obj);
		MarmotSerializers.writeLong(millis, out);
	}

	@Override
	public LocalDateTime deserialize(DataInput in) {
		return LocalDateTimes.fromUtcMillis(MarmotSerializers.readLong(in));
	}

	@Override
	public int compareBytes(Cursor cursor) {
		return cursor.compareLong();
	}
}
