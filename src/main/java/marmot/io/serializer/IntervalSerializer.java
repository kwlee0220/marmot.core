package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;

import marmot.type.DataType;
import marmot.type.Interval;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class IntervalSerializer implements ComparableMarmotSerDe<Interval> {
	@Override
	public DataType getDataType() {
		return DataType.INTERVAL;
	}
	
	@Override
	public void serialize(Interval interval, DataOutput out) {
		MarmotSerializers.writeLong(interval.getStartMillis(), out);
		MarmotSerializers.writeLong(interval.getEndMillis(), out);
	}

	@Override
	public Interval deserialize(DataInput in) {
		long start = MarmotSerializers.readLong(in);
		long end = MarmotSerializers.readLong(in);
		
		return Interval.between(start, end);
	}

	@Override
	public int compareBytes(Cursor cursor) {
		int cmp1 = cursor.compareLong();
		int cmp2 = cursor.compareLong();
		
		if ( cmp1 == 0 ) {
			return cmp2;
		}
		else {
			return cmp1;
		}
	}
}
