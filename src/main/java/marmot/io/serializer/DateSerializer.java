package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.sql.Date;

import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DateSerializer implements ComparableMarmotSerDe<Date> {
	@Override
	public DataType getDataType() {
		return DataType.DATE;
	}
	
	@Override
	public void serialize(Date obj, DataOutput out) {
		Date date = (Date)obj;
		MarmotSerializers.writeLong(date.getTime(), out);
	}

	@Override
	public Date deserialize(DataInput in) {
		long time = MarmotSerializers.readLong(in);
		return new Date(time);
	}

	@Override
	public int compareBytes(Cursor cursor) {
		return cursor.compareLong();
	}
}
