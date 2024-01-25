package marmot.io.geo.quadtree;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Lists;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class QuadTreeBuilder<T extends EnvelopedValue, P extends Partition<T>> {
	private final String m_quadKey;
	private final List<LeafNode<T,P>> m_leaves = Lists.newArrayList();
	private final Function<String,P> m_supplier;
	
	public QuadTreeBuilder(String quadKey, Function<String,P> supplier) {
		m_quadKey = quadKey;
		m_supplier = supplier;
	}
	
	public QuadTreeBuilder(Function<String,P> supplier) {
		this(null, supplier);
	}
	
	public void add(LeafNode<T,P> leaf) {
		m_leaves.add(leaf);
	}
	
	public void add(String quadKey, P partition) {
		add(new LeafNode<T,P>(quadKey, partition));
	}
	
	public QuadTree<T,P> build() {
		List<Node<T,P>> nodes = Lists.newArrayList(m_leaves);
		while ( nodes.size() > 1 ) {
			List<Node<T,P>> maxLevelNodes = findMaxLevelNodes(nodes);
			nodes.removeAll(maxLevelNodes);
			
			nodes.addAll(buildUp(maxLevelNodes));
		}
		
		Node<T,P> root = nodes.get(0);
		return new QuadTree<>(m_quadKey, root.getTileBounds(), nodes.get(0), m_supplier);
	}

	private List<NonLeafNode<T,P>> buildUp(final List<Node<T,P>> nodes) {
		List<NonLeafNode<T,P>> parents = Lists.newArrayList();
		
		Map<String,List<Node<T,P>>> groups = nodes.stream()
													.collect(Collectors.groupingBy(Node::getParentQuadKey));
		for ( Map.Entry<String,List<Node<T,P>>> ent: groups.entrySet() ) {
			String parentKey = ent.getKey();
			final List<Node<T,P>> siblings = ent.getValue();
			
			Node<T,P>[] children = new Node[QuadTree.QUAD];
			siblings.stream().forEach(n -> children[n.getSiblingOrdinal()] = n);
			IntStream.range(0, children.length)
						.filter(idx -> children[idx] == null)
						.forEach(idx -> children[idx] = new LeafNode<>(parentKey+idx,
																		m_supplier.apply(parentKey+idx)));
			
			// 자식 단말노드들 사이의 next-previous 관계를 설정한다.
			linkLeafNodes(children);
			
			parents.add(new NonLeafNode<>(ent.getKey(), children));
		}
		
		return parents;
	}
	
	private List<Node<T,P>> findMaxLevelNodes(List<? extends Node<T,P>> nodes) {
		int maxLevel = 1;
		List<Node<T,P>> maxLevelNodes = Lists.newArrayList();
		for ( Node<T,P> node: nodes ) {
			int level = node.getQuadKey().length();
			if ( level > maxLevel ) {
				maxLevelNodes = Lists.newArrayList(node);
				maxLevel = level;
			}
			else if ( level == maxLevel ) {
				maxLevelNodes.add(node);
			}
		}
		
		return maxLevelNodes;
	}
	
	private void linkLeafNodes(Node<T,P>[] siblings) {
		for ( int i = 0; i < siblings.length; ++i ) {
			Node<T,P> node = siblings[i];
			Node<T,P> prev = i > 0 ? siblings[i-1] : null;
			Node<T,P> next = i < siblings.length-1 ? siblings[i+1] : null;
			
			QuadTree.link(prev, node);
			QuadTree.link(node, next);
		}
	}
	
/*
	public static <T extends EnvelopedValue, P extends Partition<T>>
		QuadTree<T,P> buildEmptyQuadTree(List<String> quadKeys, Supplier<P> partitionSupplier) {
		Collections.sort(quadKeys);
		List<Node<T,P>> nodes = quadKeys.stream()
										.map(quadKey -> (Node<T,P>)new LeafNode(quadKey, partitionSupplier))
										.collect(Collectors.toList());
		for ( int i = 0; i < nodes.size(); ++i ) {
			LeafNode<T,P> node = (LeafNode<T,P>)nodes.get(i);
			LeafNode<T,P> prev = i > 0 ? (LeafNode<T,P>)nodes.get(i-1) : null;
			LeafNode<T,P> next = i < nodes.size()-1 ? (LeafNode<T,P>)nodes.get(i+1) : null;
			
			QuadTree.link(prev, node);
			QuadTree.link(node, next);
		}

		while ( nodes.size() > 1 ) {
			buildUp(nodes);
		}
		return new QuadTree<>(nodes.get(0), partitionSupplier);
	}

	private static <T extends EnvelopedValue, P extends Partition<T>>
		void linkLeafNodes(List<LeafNode<T,P>> leafNodes) {
		for ( int i = 0; i < leafNodes.size(); ++i ) {
			LeafNode<T,P> node = (LeafNode<T,P>)leafNodes.get(i);
			LeafNode<T,P> prev = i > 0 ? (LeafNode<T,P>)leafNodes.get(i-1) : null;
			LeafNode<T,P> next = i < leafNodes.size()-1 ? (LeafNode<T,P>)leafNodes.get(i+1) : null;
			
			QuadTree.link(prev, node);
			QuadTree.link(node, next);
		}
	}
*/
}
