package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;

import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DoubleArraySerializer implements DataTypeSerializer<Double[]> {
	@Override
	public DataType getDataType() {
		return DataType.DOUBLE_ARRAY;
	}

	@Override
	public void serialize(Double[] data, DataOutput out) {
		if ( data != null ) {
			MarmotSerializers.writeVInt(data.length, out);
			for ( double v: data ) {
				MarmotSerializers.writeDouble(v, out);
			}
		}
		else {
			MarmotSerializers.writeVInt(-1, out);
		}
	}

	@Override
	public Double[] deserialize(DataInput in) {
		int count = MarmotSerializers.readVInt(in);
		if ( count >= 0 ) {
			Double[] array = new Double[count];
			for ( int i =0; i < count; ++i ) {
				array[i] = MarmotSerializers.readDouble(in);
			}
			
			return array;
		}
		else {
			return null;
		}
	}
}
