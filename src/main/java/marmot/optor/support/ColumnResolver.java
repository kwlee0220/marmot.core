package marmot.optor.support;

import org.mvel2.integration.VariableResolver;

import marmot.Column;
import marmot.Record;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class ColumnResolver implements VariableResolver {
	private static final long serialVersionUID = -242017595179813394L;
	
	private final Record m_record;
	private final Column m_col;
	
	ColumnResolver(Record record, Column col) {
		m_record = record;
		m_col = col;
	}

	@Override
	public String getName() {
		return m_col.name();
	}

	@Override
	public Class<?> getType() {
		return m_col.type().getInstanceClass();
	}

	@Override
	public void setStaticType(@SuppressWarnings("rawtypes") Class type) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getFlags() {
		return 0;
	}

	@Override
	public Object getValue() {
		return m_record.get(m_col.ordinal());
	}

	@Override
	public void setValue(Object value) {
		m_record.set(m_col.ordinal(), value);
	}
	
	@Override
	public String toString() {
		return m_col.toString();
	}
}
