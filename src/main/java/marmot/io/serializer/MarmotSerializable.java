package marmot.io.serializer;

import java.io.DataOutput;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface MarmotSerializable {
//	public static Object deserialize(DataInput in);
	public void serialize(DataOutput out);
}
