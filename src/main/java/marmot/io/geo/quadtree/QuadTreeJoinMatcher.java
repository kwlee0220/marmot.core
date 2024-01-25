package marmot.io.geo.quadtree;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import marmot.optor.geo.SpatialQueryOperation;
import marmot.optor.support.Match;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class QuadTreeJoinMatcher<T extends EnvelopedValue, P extends Partition<T>> {
	private static final Logger s_logger = LoggerFactory.getLogger(QuadTreeJoinMatcher.class);
	
	private QuadTree<T,P> m_left;
	private QuadTree<T,P> m_right;
	
	public QuadTreeJoinMatcher(QuadTree<T,P> left, QuadTree<T,P> right) {
		m_left = left;
		m_right = right;
	}
	
	public FStream<Match<LeafNode<T,P>>> streamLeafNodeMatch() {
		return FStream.from(new MatchIterator<T,P>(m_left, m_right));
	}
	
	public FStream<Match<T>> streamMatch(SpatialQueryOperation op) {
		return streamLeafNodeMatch().flatMap(new ToValueMatchStream<T,P>(op));
	}
	
	private static class ToValueMatchStream<T extends EnvelopedValue, P extends Partition<T>>
											implements Function<Match<LeafNode<T,P>>,FStream<Match<T>>> {
		private final SpatialQueryOperation m_op;
		
		ToValueMatchStream(SpatialQueryOperation op) {
			m_op = op;
		}
		
		@Override
		public FStream<Match<T>> apply(Match<LeafNode<T,P>> nodeMatch) {
			P leftPart = nodeMatch.m_left.getPartition();
			P rightPart = nodeMatch.m_right.getPartition();
			
			List<Match<T>> matches = Lists.newArrayList();
			switch ( m_op ) {
				case INTERSECTS:
					if ( s_logger.isDebugEnabled() ) {
						leftPart.values()
								.forEach(left -> rightPart.values()
														.filter(right -> left.intersects(right))
														.forEach(right -> 
															matches.add(new Match<>(left,right))));
						s_logger.debug(String.format("matches: %dx%d -> %d (%.1f%%)",
												leftPart.size(), rightPart.size(), matches.size(),
												(double)matches.size()/(leftPart.size()*rightPart.size())*100));
						return FStream.from(matches);
					}
					else {
						return leftPart.values()
								.flatMap(lv -> rightPart.values()
														.filter(rv -> lv.intersects(rv))
														.map(rv -> new Match<>(lv, rv)));
						
					}
				case CONTAINS:
					return leftPart.values()
							.flatMap(lv -> rightPart.values()
													.filter(rv -> lv.contains(rv))
													.map(rv -> new Match<>(lv, rv)));
				case CONTAINED_BY:
					return leftPart.values()
							.flatMap(lv -> rightPart.values()
													.filter(rv -> lv.containedBy(rv))
													.map(rv -> new Match<>(lv, rv)));
				default:
					return null;
			}
		}
	}
	
	static class MatchIterator<T extends EnvelopedValue, P extends Partition<T>>
											implements Iterator<Match<LeafNode<T,P>>> {
		private final Iterator<LeafNode<T,P>> m_iterLeft;
		private final Iterator<LeafNode<T,P>> m_iterRight;
		private LeafNode<T,P> m_left;
		private LeafNode<T,P> m_right;
		private Match<LeafNode<T,P>> m_match;
		
		MatchIterator(QuadTree<T,P> left, QuadTree<T,P> right) {
			m_iterLeft = left.streamLeafNodes().iterator();
			m_iterRight = right.streamLeafNodes().iterator();
			
			m_left = m_iterLeft.hasNext() ? m_iterLeft.next() : null;
			m_right = m_iterRight.hasNext() ? m_iterRight.next() : null;
			
			m_match = matchNext();
		}

		@Override
		public boolean hasNext() {
			return m_match != null;
		}

		@Override
		public Match<LeafNode<T,P>> next() {
			Match<LeafNode<T,P>> match = m_match;
			m_match = matchNext();
			
			return match;
		}
		
		private Match<LeafNode<T,P>> matchNext() {
			Match<LeafNode<T,P>> match = null;
			while ( m_left != null && m_right != null ) {
				if ( m_left.getQuadKey().equals(m_right.getQuadKey()) ) {
					match = new Match<LeafNode<T,P>>(m_left, m_right);
	
					m_left = m_iterLeft.hasNext() ? m_iterLeft.next() : null;
					m_right = m_iterRight.hasNext() ? m_iterRight.next() : null;
				}
				else if ( m_left.getTileBounds().contains(m_right.getTileBounds()) ) {
					match = new Match<LeafNode<T,P>>(m_left, m_right);
	
					m_right = m_iterRight.hasNext() ? m_iterRight.next() : null;
				}
				else if ( m_right.getTileBounds().contains(m_left.getTileBounds()) ) {
					match = new Match<LeafNode<T,P>>(m_left, m_right);
	
					m_left = m_iterLeft.hasNext() ? m_iterLeft.next() : null;
				}
				else {
					if ( m_left.getQuadKey().compareTo(m_right.getQuadKey()) < 0 ) {
						m_left = m_iterLeft.hasNext() ? m_iterLeft.next() : null;
					}
					else {
						m_right = m_iterRight.hasNext() ? m_iterRight.next() : null;
					}
				}
				
				if ( match != null ) {
					if ( match.m_left.getValueCount() > 0 && match.m_right.getValueCount() > 0 ) {
						return match;
					}
				}
			}
			
			return null;
		}
	}
}
