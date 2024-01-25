package marmot.optor;

import java.util.List;
import java.util.Objects;

import utils.CSV;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class KeyColumn {
	private final String m_name;
	private final SortOrder m_sortOrder;
	private final NullsOrder m_nullsOrder;
	
	public static KeyColumn of(String colName) {
		return new KeyColumn(colName, SortOrder.NONE, NullsOrder.FIRST);
	}
	
	public static KeyColumn of(String colName, SortOrder sortOrder) {
		return new KeyColumn(colName, sortOrder, getDefaultNullsOrder(sortOrder));
	}
	
	public static KeyColumn of(String colName, SortOrder sortOrder, NullsOrder nullsOrder) {
		return new KeyColumn(colName, sortOrder, nullsOrder);
	}
	
	private KeyColumn(String name, SortOrder sortOrder, NullsOrder nullsOrder) {
		Utilities.checkNotNullArgument(name, "column name");
		Utilities.checkNotNullArgument(sortOrder, "sort order");
		Utilities.checkNotNullArgument(nullsOrder, "nulls order");
		
		m_name = name;
		m_sortOrder = sortOrder;
		m_nullsOrder = nullsOrder;
	}
	
	public String name() {
		return m_name;
	}
	
	public SortOrder sortOrder() {
		return m_sortOrder;
	}
	
	public NullsOrder nullsOrder() {
		return m_nullsOrder;
	}
	
	public KeyColumn duplicate() {
		return KeyColumn.of(m_name, m_sortOrder, m_nullsOrder);
	}
	
	public boolean matches(String colName) {
		return m_name.equalsIgnoreCase(colName);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || getClass() != obj.getClass() ) {
			return false;
		}
		
		KeyColumn other = (KeyColumn)obj;
		return m_name.equalsIgnoreCase(other.m_name)
			&& Objects.equals(m_sortOrder, other.m_sortOrder)
			&& Objects.equals(m_nullsOrder, other.m_nullsOrder);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_name.toLowerCase(), m_sortOrder);
	}
	
	public static KeyColumn fromString(String str) {
		SortOrder sorder = SortOrder.NONE;
		NullsOrder norder = NullsOrder.FIRST;
		String colName = null;
		
		List<String> parts = CSV.parseCsv(str, ':', '\\').toList();
		if ( parts.size() == 0 ) {
			throw new IllegalArgumentException("invalid KeyColumn expr='" + str + "'");
		}
		if ( parts.size() == 3 ) {
			sorder = SortOrder.fromString(parts.get(1));
			norder = NullsOrder.fromString(parts.get(2));
		}
		else if ( parts.size() == 2 ) {
			sorder = SortOrder.fromString(parts.get(1));
			norder = getDefaultNullsOrder(sorder);
		}
		if ( parts.size() >= 1 ) {
			colName = parts.get(0);
		}
		
		return KeyColumn.of(colName, sorder, norder);
	}
	
	@Override
	public String toString() {
		if ( m_sortOrder == SortOrder.NONE ) {
			return m_name;
		}
		else {
			return String.format("%s:%s:%s", m_name, m_sortOrder, m_nullsOrder);
		}
	}
	
	private static NullsOrder getDefaultNullsOrder(SortOrder sorder) {
		return (sorder == SortOrder.DESC) ? NullsOrder.FIRST : NullsOrder.LAST; 
	}
}