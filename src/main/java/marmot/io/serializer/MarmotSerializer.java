package marmot.io.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface MarmotSerializer<T> {
	public void serialize(T data, DataOutput out);
	public T deserialize(DataInput in);
	
	public default List<T> deserializeList(DataInput in) {
		int count = MarmotSerializers.readVInt(in);
		List<T> list = new ArrayList<>(count);
		for ( int i =0; i < count; ++i ) {
			list.add(deserialize(in));
		}
		
		return list;
	}
	
	public default void serializeList(List<T> list, DataOutput out) {
		MarmotSerializers.writeVInt(list.size(), out);
		for ( T entry: list ) {
			serialize(entry, out);
		}
	}

	public default T fromBytes(byte[] bytes) {
		Utilities.checkNotNullArgument(bytes, "bytes is null");
		
		try ( DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes)) ) {
			return deserialize(dis);
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	public default T fromBytes(byte[] bytes, int offset, int length) {
		try ( DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes, offset, length)) ) {
			return deserialize(dis);
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}
	
	public default byte[] toBytes(T data) {
		Utilities.checkNotNullArgument(data, "data is null");
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try ( DataOutputStream dos = new DataOutputStream(baos) ) {
			serialize(data, dos);
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
		
		return baos.toByteArray();
	}

	public default T fromBase64String(String encoded) {
		Utilities.checkNotNullArgument(encoded, "Base64 string");
		
		return fromBytes(Base64.getDecoder().decode(encoded));
	}
	
	public default String toBase64String(T data) {
		return Base64.getEncoder().encodeToString(toBytes(data));
	}
}
