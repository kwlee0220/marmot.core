package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;

import marmot.support.TypedObject;
import marmot.type.DataType;
import utils.Throwables;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TypedObjectSerializer implements DataTypeSerializer<TypedObject> {
	@Override
	public DataType getDataType() {
		return DataType.TYPED;
	}
	
	@Override
	public void serialize(TypedObject typed, DataOutput out) {
		MarmotSerializers.writeString(typed.getClass().getName(), out);
		try {
			typed.writeFields(out);
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	@Override
	public TypedObject deserialize(DataInput in) {
		try {
			String clsName = MarmotSerializers.readString(in);
			
			Class<?> cls = Class.forName(clsName);
			
			Objenesis objenesis = new ObjenesisStd();
			TypedObject typed = (TypedObject)objenesis.newInstance(cls);
			typed.readFields(in);
			
			return typed;
		}
		catch ( SerializationException e ) {
			throw e;
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
		catch ( Exception e ) {
			throw Throwables.toRuntimeException(e);
		}
	}
}
