package marmot.io;

import java.util.Arrays;
import java.util.Objects;

import com.google.common.base.Preconditions;

import marmot.Record;
import marmot.RecordSchema;
import marmot.optor.KeyColumn;
import marmot.optor.NullsOrder;
import marmot.optor.SortOrder;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class RecordKey implements Comparable<RecordKey> {
	private final MultiColumnKey m_keyCols;
	private final Object[] m_values;
	
	public static RecordKey from(Object[] keyValues) {
		return new RecordKey(null, keyValues);
	}
	
	public static RecordKey from(MultiColumnKey keyCols, Object[] keyValues) {
		return new RecordKey(keyCols, keyValues);
	}
	
	public static RecordKey from(MultiColumnKey keyCols, Record record) {
		Object[] keyValues = keyCols.streamKeyColumns()
									.map(kc -> record.get(kc.name()))
									.toArray(Object.class);
		return new RecordKey(keyCols, keyValues);
	}
	
	private RecordKey(MultiColumnKey keyCols, Object[] keyValues) {
		m_keyCols = keyCols;
		m_values = keyValues;
	}
	
	public MultiColumnKey getColumns() {
		return m_keyCols;
	}
	
	public Object[] getValues() {
		return m_values;
	}
	
	public Object getValueAt(int index) {
		Preconditions.checkArgument(index >= 0 && index < m_values.length);
		
		return m_values[index];
	}
	
	public void copyFrom(Object[] values) {
		System.arraycopy(values, 0, m_values, 0, m_values.length);
	}
	
	public void copyFrom(Record record) {
		m_keyCols.streamKeyColumns()
				.map(kc -> record.get(kc.name()))
				.zipWithIndex()
				.forEach(t -> m_values[t._2] = t._1);
	}
	
	public int length() {
		return m_values.length;
	}
	
	public RecordSchema toSchema(RecordSchema baseSchema) {
		return baseSchema.project(m_keyCols.getColumnNames());
	}
	
	@Override
	public int compareTo(RecordKey other) {
		if ( this == other ) {
			return 0;
		}
		
		int length = Math.min(length(), other.length());
		for ( int i =0; i < length; ++i ) {
			final KeyColumn kc = m_keyCols.getKeyColumnAt(i);
			final Object obj = m_values[i];
			final Object otherObj = other.m_values[i];

			if ( obj != null && otherObj != null ) {
				// SortKeyValue에 올 수 있는 객체는 모두 Comparable 이라는 것을 가정한다.
				@SuppressWarnings({ "rawtypes", "unchecked" })
				int cmp = ((Comparable)obj).compareTo(other.m_values[i]);
				if ( cmp != 0 ) {
					return (kc.sortOrder() == SortOrder.ASC) ? cmp : -cmp;
				}
			}
			else if ( obj == null && otherObj == null ) {
			}
			else if ( obj == null ) {
				if ( kc.sortOrder() == SortOrder.ASC ) {
					if ( kc.nullsOrder() == NullsOrder.FIRST ) {
						return -1;
					}
					else {
						return 1;
					}
				}
				else {
					if ( kc.nullsOrder() == NullsOrder.FIRST ) {
						return 1;
					}
					else {
						return -1;
					}
				}
			}
			else if ( otherObj == null ) {
				if ( kc.sortOrder() == SortOrder.ASC ) {
					if ( kc.nullsOrder() == NullsOrder.FIRST ) {
						return 1;
					}
					else {
						return -1;
					}
				}
				else {
					if ( kc.nullsOrder() == NullsOrder.FIRST ) {
						return -1;
					}
					else {
						return 1;
					}
				}
			}

		}
		
		return length() - other.length();
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_values);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || getClass() != obj.getClass() ) {
			return false;
		}
		
		RecordKey other = (RecordKey)obj;
		return Arrays.equals(m_values, other.m_values);
	}
	
	@Override
	public String toString() {
		return m_keyCols.streamKeyColumns()
						.map(KeyColumn::name)
						.zipWith(FStream.of(m_values).map(Object::toString))
						.map(t -> t._1 + ":" + t._2)
						.join(",");
	}
}
