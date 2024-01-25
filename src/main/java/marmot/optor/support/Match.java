package marmot.optor.support;

import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Match<T> {
	public T m_left;
	public T m_right;
	
	public Match(T left, T right) {
		m_left = left;
		m_right = right;
	}
	
	@Override
	public String toString() {
		return String.format("%s<->%s", m_left, m_right);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || !(obj instanceof Match) ) {
			return false;
		}
		
		Match<T> other = (Match<T>)obj;
		return m_left.equals(other.m_left) && m_right.equals(other.m_right);
	}
	
	@Override
	public int hashCode() {
		HashCodeBuilder builder = new HashCodeBuilder();
		builder.append(m_left);
		builder.append(m_right);
		return builder.toHashCode();
	}
}
