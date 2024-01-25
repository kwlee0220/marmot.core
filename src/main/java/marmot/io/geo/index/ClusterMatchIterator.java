package marmot.io.geo.index;

import java.util.Iterator;

import marmot.optor.support.Match;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class ClusterMatchIterator implements Iterator<Match<String>> {
	private final Iterator<String> m_iterLeft;
	private final Iterator<String> m_iterRight;
	private String m_left;
	private String m_right;
	private Match<String> m_match;
	
	ClusterMatchIterator(SpatialIndexedFile left, SpatialIndexedFile right) {
		m_iterLeft = left.getClusterKeyAll().stream().sorted().iterator();
		m_iterRight = right.getClusterKeyAll().stream().sorted().iterator();
		
		m_left = m_iterLeft.hasNext() ? m_iterLeft.next() : null;
		m_right = m_iterRight.hasNext() ? m_iterRight.next() : null;
		
		m_match = matchNext();
	}

	@Override
	public boolean hasNext() {
		return m_match != null;
	}

	@Override
	public Match<String> next() {
		Match<String> match = m_match;
		m_match = matchNext();
		
		return match;
	}
	
	private Match<String> matchNext() {
		Match<String> match = null;
		while ( m_left != null && m_right != null ) {
			if ( m_left.equals(m_right) ) {
				match = new Match<>(m_left, m_right);

				m_left = m_iterLeft.hasNext() ? m_iterLeft.next() : null;
				m_right = m_iterRight.hasNext() ? m_iterRight.next() : null;
			}
			else if ( m_right.startsWith(m_left) ) {	// m_right가  m_left에 포함된 경우.
				match = new Match<>(m_left, m_right);

				m_right = m_iterRight.hasNext() ? m_iterRight.next() : null;
			}
			else if ( m_left.startsWith(m_right) ) {	// m_left가  m_right에 포함된 경우.
				match = new Match<>(m_left, m_right);

				m_left = m_iterLeft.hasNext() ? m_iterLeft.next() : null;
			}
			else {
				if ( m_left.compareTo(m_right) < 0 ) {
					m_left = m_iterLeft.hasNext() ? m_iterLeft.next() : null;
				}
				else {
					m_right = m_iterRight.hasNext() ? m_iterRight.next() : null;
				}
			}
			
			if ( match != null ) {
				return match;
			}
		}
		
		return null;
	}
}