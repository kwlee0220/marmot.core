package marmot.optor.support.colexpr;

import java.io.Serializable;

import marmot.Column;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class SelectedColumnInfo implements Serializable {
	private static final long serialVersionUID = 8240040993234041372L;
	
	String m_namespace;
	Column m_column;
	String m_alias;
	
	SelectedColumnInfo(String namespace, Column col) {
		m_namespace = namespace;
		m_column = col;
		m_alias = col.name();
	}
	
	public String getNamespace() {
		return m_namespace;
	}
	
	public Column getColumn() {
		return m_column;
	}
	
	public String getAlias() {
		return m_alias;
	}
	
	public void setAlias(String name) {
		m_alias = name;
	}
	
	@Override
	public String toString() {
		String ns = (m_namespace.length() > 0) ? String.format("%s.", m_namespace) : "";
		String al = (!m_alias.equals(m_column.name())) ? String.format("(%s)", m_alias) : "";
		
		return String.format("%s%s%s", ns, m_column.name(), al);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj )  {
			return true;
		}
		else if ( obj == null || !(obj instanceof SelectedColumnInfo) ) {
			return false;
		}
		
		SelectedColumnInfo other = (SelectedColumnInfo)obj;
		return m_namespace.equals(other.m_namespace) && m_column.ordinal() == other.m_column.ordinal();
	}
}
