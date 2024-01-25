package marmot.io.geo.quadtree;

import java.util.List;
import java.util.stream.Stream;

import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import marmot.type.MapTile;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class NonLeafNode<T extends EnvelopedValue, P extends Partition<T>> extends Node<T,P> {
	private static final Logger s_logger = LoggerFactory.getLogger(NonLeafNode.class);
	
	private final Envelope m_dataBounds = new Envelope();
	private final Node<T,P>[] m_children;
	
	@SuppressWarnings("unchecked")
	NonLeafNode(String quadKey, Node<T,P>[] children) {
		super(quadKey);

		m_children = new Node[children.length];
		for ( int i =0; i < children.length; ++i ) {
			m_children[i] = children[i];
			m_dataBounds.expandToInclude(m_children[i].getDataBounds());
		}
	}

	@Override
	public int getValueCount() {
		return Stream.of(m_children).mapToInt(Node::getValueCount).sum();
	}
	
	public Node<T,P>[] getChildrenNode() {
		return m_children;
	}
	
	public LeafNode<T,P> getFirstLeafNode() {
		Node<T,P> first = m_children[0];
		if ( first instanceof NonLeafNode ) {
			return ((NonLeafNode<T,P>)first).getFirstLeafNode();
		}
		else {
			return (LeafNode<T,P>)first;
		}
	}
	
	public LeafNode<T,P> getLastLeafNode() {
		Node<T,P> last = m_children[m_children.length-1];
		if ( last instanceof NonLeafNode ) {
			return ((NonLeafNode<T,P>)last).getLastLeafNode();
		}
		else {
			return (LeafNode<T,P>)last;
		}
	}
	
	public void collectIntersectingLeafNodes(final Envelope query,
												List<LeafNode<T,P>> collecteds) {
		for ( Node<T,P> child: m_children ) {
			boolean intersects = child.getDataBounds().intersects(query);
			if ( intersects ) {
				if ( child instanceof LeafNode ) {
					collecteds.add((LeafNode<T,P>)child);
				}
				else {
					((NonLeafNode<T,P>)child).collectIntersectingLeafNodes(query,
																		collecteds);
				}
			}
		}
	}
	
	public LeafNode<T,P> getFirstIntersectsLeafNode(final Envelope query) {
		for ( int i = 0; i < m_children.length; ++i ) {
			Node<T,P> child = m_children[i];
			
			if ( child.getDataBounds().intersects(query) ) {
				if ( child instanceof LeafNode ) {
					return (LeafNode<T,P>)child;
				}
				else {
					return ((NonLeafNode<T,P>)child).getFirstIntersectsLeafNode(query);
				}
			}
		}
		
		return null;
	}

	/**
	 * 현 non-leaf 노드에 주어진 값을 삽입한다.
	 * 주어진 값이 삽입된 leaf 노드들을 반환한다. 노드가 여러 partition에 결치는 경우가
	 * 발생할 수 있기 때문에 leaf 노드의 리스트가 반환된다.
	 * 
	 * @param value	삽입할 envelope
	 * @return	삽입된 envelope가 저장된 leaf 노드들.
	 */
	public List<LeafNode<T,P>> insert(T value) {
		List<LeafNode<T,P>> inserteds = Lists.newArrayList();
		
		final Envelope envl = value.getEnvelope();
		for ( int i =0; i < m_children.length; ++i ) {
			Node<T,P> child = m_children[i];
			
			if ( child.getTileBounds().intersects(envl) ) {
				if ( child instanceof LeafNode ) {
					// 단말노드인 경우는 split이 발생할 수도 있다.
					LeafNode<T,P> leaf = (LeafNode<T,P>)child;
					if ( leaf.insert(value) ) {
						// split이 발생되지 않은 경우.
						inserteds.add(leaf);
						continue;
					}
					else {
						// split이 발생된 경우
						// split으로 생성된 parent non-leaf 노드를 child로 설정한다.
						// 아래 line에서 non-leaf 노드를 기준으로 삽입을 시도한다.
						try {
							child = leaf.split();
							m_children[i] = child;
						}
						catch ( TooBigValueException e ) {
							if ( leaf.expand() ) {
								boolean mustBeTrue = leaf.insert(value);
								Utilities.checkState(mustBeTrue, "Something wrong!!: class=" + getClass());
								inserteds.add(leaf);
								continue;
							}
							throw e;
						}
					}
				}
				
				// child가 non-leaf 노드인 경우.
				inserteds.addAll(((NonLeafNode<T,P>)child).insert(value));
			}
		}
		if ( inserteds.size() == 0 ) {
			// 본 non-leaf 노드 영역에 주어진 데이터가 포함되는 것으로 계산되지만
			// 실제 어떤 하위 노드에도 겹치지 않은 경우 -> 생길 수 없는 경우.
			s_logger.error("node={}, key={}", getQuadKey(),
							MapTile.getSmallestContainingTile(envl).getQuadKey());
			s_logger.error("envl={}, contains={}", envl, getTileBounds().contains(envl));
			for ( Node<T,P> child: m_children ) {
				s_logger.error("\t{}, inter={}", child.getQuadKey(),
												child.getTileBounds().intersects(envl));
				if ( child instanceof LeafNode ) {
					((LeafNode<T,P>)child).insert(value);
				}
				else {
					((NonLeafNode<T,P>)child).insert(value);
				}
			}
		}
		else {
			m_dataBounds.expandToInclude(envl);
		}
		
		return inserteds;
	}

	@Override
	public Envelope getDataBounds() {
		return m_dataBounds;
	}
	
	@Override
	public String toString() {
		return String.format("NonLeaf(%s:%d)", getQuadKey(), getValueCount());
	}
}
