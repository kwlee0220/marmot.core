package marmot.io.serializer;

import java.io.DataOutput;

import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface DataTypeSerializer<T> extends MarmotSerializer<T> {
	public DataType getDataType();

	public default void serializeNullable(T obj, DataOutput out) {
		if ( obj != null ) {
			MarmotSerializers.writeByte((byte)getDataType().getTypeCode().get(), out);
			serialize(obj, out);
		}
		else {
			MarmotSerializers.writeByte((byte)-getDataType().getTypeCode().get(), out);
		}
	}
}
