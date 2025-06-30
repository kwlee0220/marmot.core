package marmot.io.geo.quadtree;

import static marmot.optor.geo.SpatialRelation.ALL;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import marmot.optor.geo.SpatialRelation;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LeafNode<T extends EnvelopedValue, P extends Partition<T>> extends Node<T,P> {
	private static final Logger s_logger = LoggerFactory.getLogger(LeafNode.class);
	
	private final @Nullable Function<String,P> m_partSupplier;
	private final P m_partition;
	private LeafNode<T,P> m_prev;
	private LeafNode<T,P> m_next;
	
	LeafNode(String quadKey, P partition) {
		super(quadKey);
		
		m_partSupplier = null;
		m_partition = partition;
	}
	
	public LeafNode(String quadKey, Function<String,P> partSupplier) {
		super(quadKey);
		
		m_partSupplier = partSupplier;
		m_partition = partSupplier.apply(quadKey);
	}
	
	public P getPartition() {
		return m_partition;
	}

	@Override
	public int getValueCount() {
		return m_partition.size();
	}

	@Override
	public Envelope getDataBounds() {
		return m_partition.getBounds();
	}
	
	public FStream<T> values() {
		return m_partition.values();
	}

	public FStream<T> query(SpatialRelation op, Envelope key) {
		Preconditions.checkArgument(op == ALL || key != null,
									"input query should not be null");
		
		if ( op == ALL ) {
			return values();
		}
		else if ( op == INTERSECTS ) {	
			if ( getDataBounds().intersects(key) ) {
				return m_partition.intersects(key);
			}
			else {
				return FStream.empty();
			}
		}
		else {
			throw new RuntimeException("unsupported SpatialQueryOperation: op=" + op);
		}
	}
	
	boolean insert(T value) throws TooBigValueException {
		return insert(value, true);
	}
	
	boolean insert(T value, boolean reserveForSpeed) throws TooBigValueException {
		if ( m_partition.add(value, reserveForSpeed) ) {
			return true;
		}
		else if ( m_partition.size() == 0 ) {
			// partition이 비어있음에도 불구하고, 입력 값을 삽입할 수 없는 경우는
			// 입력 값이 너무 큰 값으로 간주한다.
			throw new TooBigValueException("value=" + value + ", partition=" + m_partition);
		}
		else {
			return false;
		}
	}
	
	boolean expand() {
		return m_partition.expand();
	}
	
	NonLeafNode<T,P> split() {
		@SuppressWarnings("unchecked")
		LeafNode<T,P>[] childNodes = IntStream.range(0, QuadTree.QUAD)
											.mapToObj(idx -> new LeafNode<T,P>(getQuadKey()+idx, m_partSupplier))
											.toArray(sz -> (LeafNode<T,P>[])new LeafNode[sz]);
		for ( int i =0; i < childNodes.length; ++i ) {
			LeafNode<T,P> node = childNodes[i];
			QuadTree.link((i == 0) ? m_prev : childNodes[i-1], node);
			QuadTree.link(node, (i == childNodes.length-1) ? m_next : childNodes[i+1]);
		}
		
		Iterator<T> iter = m_partition.values().iterator();
		while ( iter.hasNext() ) {
			T v = iter.next();
			Envelope mbr = v.getEnvelope();

			int insCount = 0;
			for ( LeafNode<T,P> leaf: childNodes ) {
				if ( leaf.getTileBounds().intersects(mbr) ) {
					boolean done = leaf.insert(v, false);
					if ( !done ) {
						throw new AssertionError("fails to split node: " + this
												+ " because parition insertion failed");
					}
					++insCount;
				}
			}
			if ( insCount == 0 ) {
				System.err.printf("target: value=%s%n", mbr);
				Envelope bounds = getTileBounds();
				String s = String.format("(%.9f, %.9f) - (%.9f, %.9f)",
											bounds.getMinX(), bounds.getMinY(),
											bounds.getMaxX(), bounds.getMaxY());
				System.err.printf("leaf=%s, %s%n" , s, bounds.intersects(mbr));
				
				for ( int i =0; i < 4; ++i ) {
					Envelope tile = childNodes[i].getTileBounds();
					String envlStr = String.format("(%.9f, %.9f) - (%.9f, %.9f), %s",
														tile.getMinX(), tile.getMinY(),
														tile.getMaxX(), tile.getMaxY(),
														childNodes[i].getQuadKey());
					System.err.printf("leaf[%d]=%s, %s%n" , i, envlStr, tile.intersects(mbr));
				}
				
				String details = "fails to split node: " + this
								+ " because value fails to move any splitted "
								+ "partition, value=" + v;
				s_logger.error(details);
				throw new AssertionError(details);
			}
		}
		int fullCopiedCnt = (int)Arrays.stream(childNodes)
										.filter(n -> n.getValueCount() == getValueCount())
										.count();
		if ( fullCopiedCnt == QuadTree.QUAD ) {
			throw new TooBigValueException("values in this leaf-node are too big for node-split");
//			throw new AssertionError("values in this leaf-node are too big for node-split");
		}
		if ( s_logger.isDebugEnabled() )  {
			String childrenStr = Stream.of(childNodes)
										.map(leaf -> leaf.m_partition.toString())
										.collect(Collectors.joining(","));
			s_logger.debug(String.format("splitted: %s, %s", getQuadKey(), childrenStr));
		}
		
		return new NonLeafNode<>(getQuadKey(), childNodes);
	}
	
	public LeafNode<T,P> getPreviousLeafNode() {
		return m_prev;
	}
	
	void setPreviousLeafNode(LeafNode<T,P> node) {
		m_prev = node;
	}
	
	public LeafNode<T,P> getNextLeafNode() {
		return m_next;
	}
	
	void setNextLeafNode(LeafNode<T,P> node) {
		m_next = node;
	}
	
	@Override
	public String toString() {
		return String.format("Leaf(%s,%s)", getQuadKey(), m_partition);
	}
}
