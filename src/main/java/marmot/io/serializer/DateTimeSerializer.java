package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.time.LocalDateTime;

import marmot.type.DataType;
import utils.Utilities;

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
		long millis = Utilities.toUTCEpocMillis((LocalDateTime)obj);
		MarmotSerializers.writeLong(millis, out);
	}

	@Override
	public LocalDateTime deserialize(DataInput in) {
		return Utilities.fromUTCEpocMillis(MarmotSerializers.readLong(in)).toLocalDateTime();
	}

	@Override
	public int compareBytes(Cursor cursor) {
		return cursor.compareLong();
	}
}
