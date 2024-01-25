package marmot.io.serializer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import utils.func.Tuple;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface ComparableMarmotSerDe<T> extends DataTypeSerializer<T> {
	public static class Cursor {
		byte[] m_buf1;
		int m_offset1;
		byte[] m_buf2;
		int m_offset2;
		
		public Cursor(byte[] buf1, int offset1, byte[] buf2, int offset2) {
			m_buf1 = buf1;
			m_offset1 = offset1;
			m_buf2 = buf2;
			m_offset2 = offset2;
		}
		
		public void advance(int step) {
			m_offset1 += step;
			m_offset2 += step;
		}
		
		public byte readByte() {
			byte b = m_buf1[m_offset1];
			++m_offset1;
			++m_offset2;
			
			return b;
		}
		
		public Tuple<Byte,Byte> readBytePair() {
			byte b1 = m_buf1[m_offset1];
			byte b2 = m_buf2[m_offset2];
			++m_offset1;
			++m_offset2;
			
			return Tuple.of(b1, b2);
		}
		
		public int readUnsignedShort() {
			int v = WritableComparator.readUnsignedShort(m_buf1, m_offset1);
			m_offset1 += 2;
			m_offset2 += 2;
			
			return v;
		}
		
		public int compareInt() {
			int cmp = IntWritable.Comparator.compareBytes(m_buf1, m_offset1, 4,
															m_buf2, m_offset2, 4);
			m_offset1 += 4;
			m_offset2 += 4;
			
			return cmp;
		}
		
		public int compareVInt() {
			try {
				int v1 = WritableComparator.readVInt(m_buf1, m_offset1);
				int v2 = WritableComparator.readVInt(m_buf2, m_offset2);
				m_offset1 += WritableUtils.getVIntSize(v1);
				m_offset2 += WritableUtils.getVIntSize(v2);

				return ((Integer)v1).compareTo(v2);
			}
			catch ( IOException e ) {
				throw new RuntimeException(e);
			}
		}
		
		public int compareLong() {
			int cmp = LongWritable.Comparator.compareBytes(m_buf1, m_offset1, 8,
															m_buf2, m_offset2, 8);
			m_offset1 += 8;
			m_offset2 += 8;
			
			return cmp;
		}
	}
	
	public int compareBytes(Cursor cursor);
}
