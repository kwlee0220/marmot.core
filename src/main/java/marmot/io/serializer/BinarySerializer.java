package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;

import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BinarySerializer implements DataTypeSerializer<byte[]> {
	@Override
	public DataType getDataType() {
		return DataType.BINARY;
	}
	
	@Override
	public void serialize(byte[] obj, DataOutput out) {
		MarmotSerializers.writeBinary(obj, out);
	}

	@Override
	public byte[] deserialize(DataInput in) {
		return MarmotSerializers.readBinary(in);
	}
}
