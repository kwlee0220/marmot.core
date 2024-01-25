package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StringSerializer implements ComparableMarmotSerDe<String> {
	@Override
	public DataType getDataType() {
		return DataType.STRING;
	}
	
	@Override
	public void serialize(String data, DataOutput out) {
		MarmotSerializers.writeString(data, out);
	}

	@Override
	public String deserialize(DataInput in) {
		return MarmotSerializers.readString(in);
	}

	@Override
	public int compareBytes(Cursor cursor) {
		try {
			int o1Len = WritableComparator.readVInt(cursor.m_buf1, cursor.m_offset1);
			o1Len += WritableUtils.getVIntSize(o1Len);
			int o2Len = WritableComparator.readVInt(cursor.m_buf2, cursor.m_offset2);
			o2Len += WritableUtils.getVIntSize(o2Len);
			int cmp = Text.Comparator.compareBytes(cursor.m_buf1, cursor.m_offset1, o1Len,
													cursor.m_buf2, cursor.m_offset2, o2Len);
			
			cursor.m_offset1 += o1Len;
			cursor.m_offset2 += o2Len;
			
			return cmp;
		}
		catch ( IOException ignored ) {
			throw new RuntimeException("Should not be here!!", ignored);
		}
	}
}
