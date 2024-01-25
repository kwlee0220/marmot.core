package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;

import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FloatArraySerializer implements DataTypeSerializer<Float[]> {
	@Override
	public DataType getDataType() {
		return DataType.FLOAT_ARRAY;
	}

	@Override
	public void serialize(Float[] data, DataOutput out) {
		if ( data != null ) {
			MarmotSerializers.writeVInt(data.length, out);
			for ( Float v: data ) {
				MarmotSerializers.writeFloat(v, out);
			}
		}
		else {
			MarmotSerializers.writeVInt(-1, out);
		}
	}

	@Override
	public Float[] deserialize(DataInput in) {
		int count = MarmotSerializers.readVInt(in);
		if ( count >= 0 ) {
			Float[] array = new Float[count];
			for ( int i =0; i < count; ++i ) {
				array[i] = MarmotSerializers.readFloat(in);
			}
			
			return array;
		}
		else {
			return null;
		}
	}
}
