package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.time.Duration;

import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DurationSerializer implements ComparableMarmotSerDe<Duration> {
	@Override
	public DataType getDataType() {
		return DataType.DURATION;
	}
	
	@Override
	public void serialize(Duration obj, DataOutput out) {
		MarmotSerializers.writeLong(obj.getSeconds(), out);
		MarmotSerializers.writeVInt(obj.getNano(), out);
	}

	@Override
	public Duration deserialize(DataInput in) {
		long secs = MarmotSerializers.readLong(in);
		int nanos = MarmotSerializers.readVInt(in);
		
		return Duration.ofSeconds(secs, nanos);
	}

	@Override
	public int compareBytes(Cursor cursor) {
		int cmp1 = cursor.compareLong();
		int cmp2 = cursor.compareVInt();
		
		return cmp1 == 0 ? cmp2 : cmp1;
	}
}
