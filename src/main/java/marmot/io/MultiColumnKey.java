package marmot.io;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import utils.CSV;
import utils.Utilities;
import utils.stream.FStream;

import marmot.Column;
import marmot.RecordSchema;
import marmot.optor.KeyColumn;
import marmot.optor.NullsOrder;
import marmot.optor.SortOrder;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class MultiColumnKey implements Serializable {
	private static final long serialVersionUID = 1L;

	public static final MultiColumnKey EMPTY = new MultiColumnKey(Lists.newArrayList());
	
	private transient final List<KeyColumn> m_keyColumns;

	public static MultiColumnKey of(List<KeyColumn> keyColumns) {
		return new MultiColumnKey(keyColumns);
	}
	
	public static MultiColumnKey of(String colName, SortOrder sortOrder,
									NullsOrder nullsOrder) {
		KeyColumn kc = KeyColumn.of(colName, sortOrder, nullsOrder);
		return new MultiColumnKey(Lists.newArrayList(kc));
	}
	
	public static MultiColumnKey of(String colName, SortOrder sortOrder) {
		switch ( sortOrder ) {
			case DESC:
			case NONE:
				return of(colName, sortOrder, NullsOrder.FIRST);
			case ASC:
				return of(colName, sortOrder, NullsOrder.LAST);
			default:
				throw new AssertionError();
		}
	}
	
	public static MultiColumnKey of(String... colNames) {
		return of(FStream.of(colNames).map(KeyColumn::of).toList());
	}
	
	public static MultiColumnKey ofNames(List<String> colNames) {
		return of(FStream.from(colNames).map(KeyColumn::of).toList());
	}

	private MultiColumnKey(List<KeyColumn> keyColumns) {
		m_keyColumns = keyColumns;
	}
	
	public int length() {
		return m_keyColumns.size();
	}
	
	/**
	 * 주어진 이름의 컬럼의 존재 여부를 반환한다.
	 * 
	 * @param colName	컬럼 이름
	 * @return 키 존재 여부.
	 */
	public boolean existsKeyColumn(String name) {
		Utilities.checkNotNullArgument(name, "column name");
		
		return m_keyColumns.stream()
							.anyMatch(col -> col.matches(name));
	}
	
	public List<String> getColumnNames() {
		return FStream.from(m_keyColumns).map(KeyColumn::name).toList();
	}
	
	public FStream<KeyColumn> streamKeyColumns() {
		return FStream.from(m_keyColumns);
	}
	
	public KeyColumn getKeyColumnAt(int idx) {
		Utilities.checkArgument(idx >= 0 && idx < m_keyColumns.size(),
								() -> String.format("invalid column index: %d, this=", idx, this));
		
		return m_keyColumns.get(idx);
	}
	
	public KeyColumn getKeyColumn(String name) {
		Utilities.checkNotNullArgument(name, "column name is null");
		
		return m_keyColumns.stream()
							.filter(col -> col.matches(name))
							.findFirst().orElse(null);
	}
	
	public MultiColumnKey complement(RecordSchema schema) {
		Utilities.checkNotNullArgument(schema, "schema is null");
		
		return MultiColumnKey.ofNames(schema.complement(getColumnNames())
											.streamColumns()
											.map(Column::name)
											.toList());
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || getClass() != obj.getClass() ) {
			return false;
		}
		
		MultiColumnKey other = (MultiColumnKey)obj;
		return m_keyColumns.equals(other.m_keyColumns);
	}
	
	@Override
	public int hashCode() {
		return m_keyColumns.hashCode();
	}
	
	public static MultiColumnKey fromString(String colSpecList) {
		Utilities.checkNotNullArgument(colSpecList, "string representation is null");
		
		List<KeyColumn> keyCols = CSV.parseCsv(colSpecList)
										.map(KeyColumn::fromString)
										.toList();
		return new MultiColumnKey(keyCols);
	}
	
	@Override
	public String toString() {
		return m_keyColumns.stream()
							.map(KeyColumn::toString)
							.collect(Collectors.joining(","));
	}
	
	public static MultiColumnKey concat(MultiColumnKey... keys) {
		Utilities.checkNotNullArguments(keys, "MultiColumnKey array is null");
		
		List<KeyColumn> keyCols = FStream.of(keys)
										.flatMap(k -> FStream.from(k.m_keyColumns))
										.collect(Lists.newArrayList(), (a,ks)->a.add(ks));
		return new MultiColumnKey(keyCols);
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = -6864353473263853643L;
		
		private final String m_strRep;
		
		private SerializationProxy(MultiColumnKey mcKey) {
			m_strRep = mcKey.toString();
		}
		
		private Object readResolve() {
			return MultiColumnKey.fromString(m_strRep);
		}
	}
}
