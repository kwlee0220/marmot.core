package marmot.optor.support;

import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import marmot.io.geo.quadtree.EnvelopedValue;
import marmot.io.geo.quadtree.LeafNode;
import marmot.io.geo.quadtree.Partition;
import marmot.io.geo.quadtree.QuadTree;
import marmot.io.geo.quadtree.QuadTreeBuilder;
import marmot.type.MapTile;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class QuadKeyBinder {
	private static final Logger s_logger = LoggerFactory.getLogger(QuadKeyBinder.class);
	
	private final Collection<String> m_qkeys;
	private final boolean m_bindOutlier;
	private final boolean m_bindOwner;
	private List<LeafInfo> m_leafInfos;
	private QuadTree<VoidValue,VoidPartition> m_qtree;
	
	private static class LeafInfo {
		private final String m_quadKey;
		private final Envelope m_bounds;
		
		LeafInfo(String quadKey, Envelope bounds) {
			m_quadKey = quadKey;
			m_bounds = bounds;
		}
	}
	
	public QuadKeyBinder(Collection<String> qkeys, boolean bindOutlier) {
		this(qkeys, bindOutlier, false);
	}
	
	public QuadKeyBinder(Collection<String> qkeys, boolean bindOutlier, boolean bindOwner) {
		m_qkeys = qkeys;
		m_bindOutlier = bindOutlier;
		m_bindOwner = bindOwner;

		m_leafInfos = FStream.from(m_qkeys)
							.filter(k -> !k.equals("outliers"))
							.map(qk -> new LeafInfo(qk, MapTile.fromQuadKey(qk).getBounds()))
							.toList();
		
		// input RecordSet에 포함된 레코드에 해당하는 quad-key를 신속하게
		// 접근하기 위한 quad-tree를 생성한다.
		m_qtree = buildQuadKeyIndex(m_qkeys);
	}
	
	public FStream<String> streamQuadKeys(Envelope envl84) {
		return FStream.from(m_leafInfos)
						.filter(info -> info.m_bounds.contains(envl84))
						.map(info -> info.m_quadKey);
	}
	
	public List<QuadKeyBinding> bindQuadKeys(Envelope envl84) {
		if ( envl84.getMaxY() > 85 || envl84.getMinY() < -85
				|| envl84.getMaxX() > 180 || envl84.getMinX()< -180 ) {
			s_logger.debug("invalid input for QuadKeyBinder: input={}", envl84);

			return Collections.emptyList();
		}
		
		List<QuadKeyBinding> bindings;
		if ( m_bindOwner ) {
			Coordinate refPt = envl84.centre();
			List<LeafNode<VoidValue,VoidPartition>> leaves
										= m_qtree.queryLeafNodes(INTERSECTS, envl84);
			if ( leaves.size() == 0 ) {
				bindings = Collections.emptyList();
			}
			else {
				bindings = FStream.from(leaves)
								.filter(leaf -> leaf.getTileBounds().contains(refPt))
								.map(leaf -> new QuadKeyBinding(envl84, leaf.getQuadKey()))
								.toList();
				if ( bindings.size() != 1 ) {
					System.err.println("SOMETHING WRONG XXXXXXXXXXXXXXXXXXXXXXXXXXX");
					System.exit(-1);
				}
			}
		}
		else {
			bindings = FStream.from(m_qtree.queryLeafNodes(INTERSECTS, envl84))
							.map(LeafNode::getQuadKey)
							.map(qk -> new QuadKeyBinding(envl84, qk))
							.toList();
		}
		if ( bindings.isEmpty() && m_bindOutlier ) {
			bindings = FStream.from(getOutlierQuadKey(envl84))
								.map(qkey -> new QuadKeyBinding(envl84, qkey))
								.toList();
		}
		
		return bindings;
	}
	
	private List<String> getOutlierQuadKey(Envelope envl84) {
		// 첫번째 맵리듀스 단계의 sampling에서 이상하게 sampling하게 되면
		// 앞선 'attach.m_quadKeySource'에 기록되지 않은 quadkey를 갖는
		// 데이터가 존재할 수 있다. 이런 경우는 이 데이터에 대한 quad-key를
		// 직접 계산하여 데이터를 출력한다.
		//
		String rootQKey = m_qtree.getQuadKey();
		MapTile tile = MapTile.getSmallestContainingTile(envl84);
		String quadKey = tile.getQuadKey();
		
		// mbr 값이 이상한 경우 quadKey값이 ""가 반환될 수 있다.
		if ( quadKey.equals("") ) {
			return Collections.singletonList(quadKey);
		}

		// 구해진 quadkey가 너무 잘게 나뉘어진 경우 (즉, quadkey의 길이가 너무 긴 경우)
		// 가장 가까운 동일 조상을 갖는 quadkey들과 같은 자식이 되도록 quadkey 길이를 줄인다. 
		
		// 구해진 quadkey와 가장 긴 prefix를 공유하는 quadkey들을 찾는다.
		int prefixLen = m_qkeys.stream()
								.mapToInt(k -> Strings.commonPrefix(quadKey, k).length())
								.max()
								.getAsInt();
		
		List<String> quadKeys;
		if ( quadKey.length() > prefixLen ) {
			quadKeys = Collections.singletonList(quadKey.substring(0, prefixLen+1));
		}
		else {
			quadKeys = FStream.range(0, 4)
							.map(idx -> quadKey + idx)
							.filter(k -> MapTile.fromQuadKey(k).intersects(envl84))
							.toList();
		}
		
		s_logger.debug("outlier: cluster={}, data={}, qkey={}", rootQKey, quadKey, quadKeys);
		return quadKeys;
	}
	
	private static QuadTree<VoidValue,VoidPartition> buildQuadKeyIndex(Collection<String> quadKeys) {
		Function<String,VoidPartition> supplier = qkey->new VoidPartition();
		QuadTreeBuilder<VoidValue, VoidPartition> builder = new QuadTreeBuilder<>(supplier);
		FStream.from(quadKeys)
				// 'outliers'는 공간 정보가 없는 것이므로 제외시킨다.
				.filter(k -> !k.equals("outliers"))
				.forEach(quadKey -> {
					VoidPartition part = supplier.apply(quadKey);
					Envelope bounds = MapTile.fromQuadKey(quadKey).getBounds();
					part.setDataBounds(bounds);
		
					builder.add(quadKey, part);
				});
		QuadTree<VoidValue,VoidPartition> qtree = builder.build();
		qtree.setRangeExpandable(false);
		
		return qtree;
	}
	
	public final static class QuadKeyBinding {
		private final String m_quadKey;
		private final Envelope m_mbr4326;
		
		private QuadKeyBinding(Envelope mbr4326, String quadKey) {
			m_mbr4326 = mbr4326;
			m_quadKey = quadKey;
		}
		
		public Envelope mbr4326() {
			return m_mbr4326;
		}
		
		public String quadkey() {
			return m_quadKey;
		}
		
		@Override
		public String toString() {
			return String.format("quadkey_binding[key=%s]", m_quadKey);
		}
	}
	
	private static class VoidValue implements EnvelopedValue {
		private static final Envelope NULL = new Envelope();
		
		@Override
		public Envelope getEnvelope() {
			return NULL;
		}
	}
	
	private static class VoidPartition implements Partition<VoidValue> {
		private Envelope m_bounds = new Envelope();
		
		void setDataBounds(Envelope bounds) {
			m_bounds = bounds;
		}

		@Override
		public Envelope getBounds() {
			return m_bounds;
		}
		
		@Override
		public boolean add(VoidValue value) {
			return false;
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public FStream<VoidValue> values() {
			return FStream.empty();
		}
		
		@Override
		public String toString() { return ""; }
	}
}
