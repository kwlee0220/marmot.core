package marmot.io.geo.quadtree;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import marmot.optor.geo.SpatialRelation;
import marmot.type.MapTile;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class QuadTree<T extends EnvelopedValue, P extends Partition<T>> {
	private static final Logger s_logger = LoggerFactory.getLogger(QuadTree.class);
	public static final int QUAD = 4;
	
	// QuadTree의 최대 영역이 설정된 경우
	private String m_enforcedQuadKey;	// QuadTree에 설정된 QuadKey
	private Envelope m_enforcedBounds;	// QuadTree에 포함된 모든 데이터를 포함하는 최소 넓이 타일의 사각영역
	
	private Node<T,P> m_root;
	private final Function<String,P> m_partitionSupplier;
	private boolean m_rangeExpandable = true;
	
	/**
	 * QuadTree를 생성한다.
	 * 
	 * @param quadKey	QuadTree에 부여할 quad-key.
	 * @param partitionSupplier	생성될 QuadTree가 사용할 partition 생성기.
	 * 						데이터 삽입과정에서 새로운 단말노드가 생성되어 partition이 필요할 때 활용된다.
	 */
	public QuadTree(String quadKey, Function<String,P> partitionSupplier) {
		m_enforcedQuadKey = quadKey;
		m_enforcedBounds = MapTile.fromQuadKey(m_enforcedQuadKey).getBounds();
		
		m_root = new LeafNode<T,P>(quadKey, partitionSupplier);
		m_partitionSupplier = partitionSupplier;
	}
	
	/**
	 * QuadTree를 생성한다.
	 * 
	 * @param partitionSupplier	생성될 QuadTree가 사용할 partition 생성기.
	 * 						데이터 삽입과정에서 새로운 단말노드가 생성되어 partition이 필요할 때 활용된다.
	 */
	public QuadTree(Function<String,P> partitionSupplier) {
		m_enforcedQuadKey = null;
		m_enforcedBounds = null;
		m_root = new LeafNode<T,P>("", partitionSupplier);
		m_partitionSupplier = partitionSupplier;
	}
	
	/**
	 * QuadTree를 생성한다.
	 * 
	 * @param root	생성될 QuadTree의 최상위 노드.
	 * @param partitionSupplier	partition 생성기.
	 */
	public QuadTree(Node<T,P> root, Function<String,P> partitionSupplier) {
		Utilities.checkNotNullArgument(root, "root is null");

		m_enforcedQuadKey = null;
		m_enforcedBounds = null;
		m_root = root;
		m_partitionSupplier = partitionSupplier;
	}
	
	QuadTree(String quadKey, Envelope bounds, Node<T,P> root, Function<String,P> partitionSupplier) {
		Utilities.checkNotNullArgument(root, "root is null");

		m_enforcedQuadKey = quadKey;
		m_enforcedBounds = bounds;
		m_root = root;
		m_partitionSupplier = partitionSupplier;
	}
	
	/**
	 * QuadTree에 포함된 모든 value들을 포함하는 타일 중 가장 작은 타일의 quad-key를 반환한다.
	 * 
	 * @return	quad-key값
	 */
	public String getQuadKey() {
		return m_enforcedQuadKey != null ? m_enforcedQuadKey : m_root.getQuadKey();
	}

	/**
	 * QuadTree에 포함된 모든 value들을 포함하는 타일 중 가장 사각형을 반환한다.
	 * 사각형의 좌표 값은 EPSG:4326 좌표체계로 표현된다.
	 * 
	 * @return	사각형 좌표.
	 */
	public Envelope getDataBounds() {
		return m_root.getDataBounds();
	}
	
	/**
	 * QuadTree에 포함된 모든 value들을 포함하는 타일 중 가장 사각형을 반환한다.
	 * 사각형의 좌표 값은 EPSG:4326 좌표체계로 표현된다.
	 * {@link #getDataBounds()}와의 차이점은 QuadTree의 bounds가 고정된 경우,
	 * {@link #getDataBounds()}은 실제 저장된 모든 value들의 MBR를 반환하는 반면,
	 * {@link #getBounds()}는 설정된 bounds를 반환한다.
	 * 
	 * @return	사각형 좌표.
	 */
	public Envelope getBounds() {
		return m_enforcedQuadKey != null ? m_enforcedBounds : m_root.getTileBounds();
	}
	
	public boolean getRangeExpandable() {
		return m_rangeExpandable;
	}
	
	public void setRangeExpandable(boolean flag) {
		if ( m_rangeExpandable = flag ) {
			m_enforcedQuadKey = null;
		}
		else {
			m_enforcedQuadKey = m_root.getQuadKey();
			m_enforcedBounds = MapTile.fromQuadKey(m_enforcedQuadKey).getBounds();
		}
	}
	
	/**
	 * 입력 공간 데이터를 삽입하고, 데이터가 삽입된 단말 노드를 반환한다.
	 * 만일 복수 개의 단말 노드에 삽입된 경우는 모든 단말노드를 반환한다.
	 * 이때, 순서는 노드의 quad-key의 순서에 따라 반환된다.
	 * 
	 * @param value	삽입할 공간 데이터.
	 * @return	삽입된 공간 데이터가 포함된 단말 노드 리스트.
	 * @throws TooBigValueException	삽입할 데이터가 너무커서 단말노드에 저장할 수 없는 경우.
	 */
	public List<LeafNode<T,P>> insert(T value) throws TooBigValueException {
		Preconditions.checkArgument(value != null);
		
		checkForExpand(value);
		
		while ( m_root instanceof LeafNode ) {
			LeafNode<T,P> lroot = (LeafNode<T,P>)m_root;
			if ( lroot.insert(value) ) {
				List<LeafNode<T,P>> leafs = Lists.newArrayList();
				leafs.add(lroot);
				
				return leafs;
			}
			
			// 현재 단말노드에 더 이상 값을 넣을 수 없는 경우는 노드를 분할시킨다.
			NonLeafNode<T,P> parent = lroot.split();
			
			// 분할된 모든 데이터가 하나의 split에만 저장되는 경우,
			// 최상위 노드의 tile을 해당 split의 tile로 축소시킨다.
			List<Node<T,P>> nonEmptyChilds = FStream.of(parent.getChildrenNode())
													.filter(n -> n.getValueCount()>0)
													.toList();
			if ( nonEmptyChilds.size() > 1 ) {
				m_root = parent;
			}
			else {
				m_root = nonEmptyChilds.get(0);
				if ( m_root instanceof LeafNode ) {
					lroot = (LeafNode<T,P>)m_root;
					lroot.setPreviousLeafNode(null);
					lroot.setNextLeafNode(null);
				}
				
				checkForExpand(value);
			}
		}
		
		return ((NonLeafNode<T,P>)m_root).insert(value);
	}
	
	/**
	 * 본 quad-tree에 포함된 단말 노드 중에서 quad-key 순으로 가장 작은 값의 단말 노드를 반환한다.
	 * 
	 * @return	단말노드
	 */
	public LeafNode<T,P> getFirstLeafNode() {
		return (m_root instanceof LeafNode) ? (LeafNode<T,P>)m_root
											: ((NonLeafNode<T,P>)m_root).getFirstLeafNode();
	}
	
	/**
	 * 본 quad-tree에 포함된 단말 노드 중에서 quad-key 순으로 가장 큰 값의 단말 노드를 반환한다.
	 * 
	 * @return	단말노드
	 */
	public LeafNode<T,P> getLastLeafNode() {
		return (m_root instanceof LeafNode) ? (LeafNode<T,P>)m_root
											: ((NonLeafNode<T,P>)m_root).getLastLeafNode();
	}

	/**
	 * QuadTree에 저장된 모든 단말 노드들의 순환자를 반환한다.
	 * 
	 * @return	단말 노드 순환자
	 */
	public FStream<LeafNode<T,P>> streamLeafNodes() {
		return FStream.from(new LeafNodeIterator<>(this));
	}

	/**
	 * 본 quad-tree에 포함된 단말 노드 중에서 주어진 box와 겹치는 단말 노드 중 quad-key 순으로
	 * 가장 작은 값의 단말 노드를 반환한다.
	 * 주어진 box와 단말노드의 tile 영역과의 겹치는 여부만을 확인하기 때문에, 단말 노드의 partition에
	 * 포함된 데이터 중에는 실제로 겹치는 데이터가 없을 수도 있다.
	 * 질의 사각형 ({@code query})은 반드시 EPSG:4326 좌표체계로 기술되어야 한다.
	 * 
	 * @param op	공간 질의 연산자
	 * @param key	질의 box.
	 * @return	단말노드. 겹치는 단말 노드가 없는 경우는 null이 반환된다.
	 */
	public List<LeafNode<T,P>> queryLeafNodes(SpatialRelation op, Envelope key) {
		List<LeafNode<T,P>> foundList = Lists.newArrayList();
		
		if ( op == SpatialRelation.INTERSECTS ) {
			if ( m_root instanceof LeafNode ) {
				boolean intersects = m_root.getDataBounds().intersects(key);
				if ( intersects ) {
					foundList.add((LeafNode<T,P>)m_root);
				}
			}
			else {
				((NonLeafNode<T,P>)m_root).collectIntersectingLeafNodes(key, foundList);
			}
		}
		else if ( op == SpatialRelation.ALL ) {
			return streamLeafNodes().toList();
		}
		else {
			throw new RuntimeException("unsupported query operation: op=" + op);
		}
		
		return foundList;
	}
	
	/**
	 * QuadTree에 삽입된 모든 데이터들의 스트림을 반환한다.
	 * 
	 * @return	테이터 스트림
	 */
	public FStream<T> streamValues() {
		return streamLeafNodes().flatMap(LeafNode::values);
	}

	/**
	 * QuadTree에 저장된 모든 데이터 중 주어진 질의 사각형과 겹치는 데이터를 접근하는 순환자를 반환한다.
	 * 질의 사각형 ({@code query})은 반드시 EPSG:4326 좌표체계로 기술되어야 한다.
	 * 
	 * @param op	공간 질의 연산자
	 * @param key	질의 box.
	 * @return	데이터 순환자
	 */
	public FStream<T> query(SpatialRelation op, Envelope keyWgs84) {
		return FStream.from(queryLeafNodes(op, keyWgs84))
						.flatMap(node -> node.query(op, keyWgs84));
	}
	
	public void compact() {
		String mbrQuadKey = MapTile.getSmallestContainingTile(m_root.getDataBounds()).getQuadKey();
		String quadKey = getQuadKey();
		
		if ( mbrQuadKey.length() > quadKey.length() && mbrQuadKey.startsWith(quadKey) ) {
			if ( m_root instanceof NonLeafNode ) {
				m_root = new NonLeafNode<>(mbrQuadKey,
										((NonLeafNode<T,P>)m_root).getChildrenNode());
			}
			else if ( m_root instanceof LeafNode ) {
				LeafNode<T,P> leaf = (LeafNode<T,P>)m_root;
				m_root = new LeafNode<>(mbrQuadKey, leaf.getPartition());
			}
			else {
				throw new AssertionError();
			}
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s", getQuadKey());
	}
	
	private void checkForExpand(T value) {
		Envelope mbr = value.getEnvelope();
		
		if ( m_rangeExpandable ) {
			// 삽입할 공간이 quadtree 전체 공간에 포함되지 않는 경우는 
			// quadtree의 level을 감소하여 포함하도록 한다.
			while ( !m_root.getTileBounds().contains(mbr) ) {
				expandToInsert(value);
			}
		}
		else {
			if ( !mbr.intersects(m_enforcedBounds) ) {
				String vqk = MapTile.getSmallestContainingTile(mbr).getQuadKey();
				throw new OutOfRangeException("tree=" + m_enforcedQuadKey + ", value=" + vqk);
			}
			
			while ( !m_root.getTileBounds().contains(mbr)
					&& m_root.getQuadKey().length() > m_enforcedQuadKey.length() ) {
				expandToInsert(value);
			}
		}
	}
	
	private void expandToInsert(T value) {
		String parentKey = m_root.getParentQuadKey();
		@SuppressWarnings("unchecked")
		Node<T,P>[] siblings = IntStream.range(0, QUAD)
										.mapToObj(idx -> parentKey + idx)
										.map(k -> {
											if ( k.equals(m_root.getQuadKey()) ) {
												return m_root;
											}
											else {
												return new LeafNode<>(k, m_partitionSupplier);
											}
										})
										.toArray(sz -> (Node<T,P>[])new Node[sz]);
		for ( int i =0; i < siblings.length; ++i ) {
			Node<T,P> node = siblings[i];
			link((i>0) ? siblings[i-1] : null, node);
			link(node, (i<siblings.length-1) ? siblings[i+1] : null); 
		}
		m_root = new NonLeafNode<>(parentKey, siblings);

		s_logger.debug("expanded: {}", this);
	}
	
	static <T extends EnvelopedValue, P extends Partition<T>> void link(Node<T,P> prev, Node<T,P> next) {
		LeafNode<T,P> lprev = (prev != null && prev instanceof NonLeafNode)
							? ((NonLeafNode<T,P>)prev).getLastLeafNode()
							: (LeafNode<T,P>)prev;
		LeafNode<T,P> lnext = (next != null && next instanceof NonLeafNode)
				? ((NonLeafNode<T,P>)next).getFirstLeafNode()
				: (LeafNode<T,P>)next;
				
		if ( lprev != null ) {
			lprev.setNextLeafNode(lnext);
		}
		if ( lnext != null ) {
			lnext.setPreviousLeafNode(lprev);
		}
	}
	
	private static class LeafNodeIterator<T extends EnvelopedValue, P extends Partition<T>>
																		implements Iterator<LeafNode<T,P>> {
		private LeafNode<T,P> m_next;
		
		private LeafNodeIterator(QuadTree<T,P> tree) {
			m_next = tree.getFirstLeafNode();
		}

		@Override
		public boolean hasNext() {
			return m_next != null;
		}

		@Override
		public LeafNode<T,P> next() {
			LeafNode<T,P> ret = m_next;
			
			m_next = m_next.getNextLeafNode();
			return ret;
		}
	}
}
