package marmot.optor.geo.join;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.geo.cluster.CacheableQuadCluster;
import marmot.io.geo.cluster.QuadClusterCache;
import marmot.io.geo.cluster.QuadClusterFile;
import marmot.optor.geo.SpatialRelation;
import marmot.optor.support.JoinUtils;
import marmot.optor.support.QuadKeyBinder;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.plan.SpatialJoinOptions;
import marmot.rset.AbstractRecordSet;
import marmot.support.EnvelopeTaggedRecord;
import marmot.support.ProgressReportable;
import utils.StopWatch;
import utils.UnitUtils;
import utils.func.KeyValue;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ClusteredNLJoinRecordSet extends AbstractRecordSet implements ProgressReportable {
	private static final Logger s_logger = LoggerFactory.getLogger(ClusteredNLJoinRecordSet.class);
	
	private static int MAX_WINDOW_SIZE = 4 * 1024;
	private static int MIN_WINDOW_SIZE = 32;
	
	private RecordSet m_input;
	private final QuadClusterCache m_rightClusterCache;
	private final SpatialJoinMatcher m_sjMatcher;
	private final ColumnSelector m_selector;

	private CacheableQuadCluster m_cluster;
	private final FStream<Record> m_output;
	private final Map<String,Record> m_binding = Maps.newHashMap();

	private long m_leftRecCount = 0;	// outer record read count
	private long m_outputCount = 0;
	protected long m_elapsed;
	private boolean m_finalProgressReported = false;

	public ClusteredNLJoinRecordSet(String leftGeomColName, RecordSet left,
									QuadClusterFile<? extends CacheableQuadCluster> rightIdxFile,
									SpatialJoinOptions opts) {
		this(leftGeomColName, left.getRecordSchema(), left.fstream(), rightIdxFile, opts);
		
		m_input = left;
	}

	public ClusteredNLJoinRecordSet(String leftGeomColName, RecordSchema leftSchema, FStream<Record> left,
									QuadClusterFile<? extends CacheableQuadCluster> rightIdxFile,
									SpatialJoinOptions opts) {
		m_rightClusterCache = new QuadClusterCache(rightIdxFile);
		RecordSchema rightSchema = rightIdxFile.getRecordSchema();
		int rightGeomColIdx = rightIdxFile.getGRecordSchema().getGeometryColumnIdx();
		String srid = rightIdxFile.getGRecordSchema().getSrid();

		int leftGeomColIdx = leftSchema.getColumn(leftGeomColName).ordinal();
		SpatialRelation joinExpr = opts.joinExpr().map(SpatialRelation::parse)
										.getOrElse(SpatialRelation.INTERSECTS);
		m_sjMatcher = SpatialJoinMatchers.from(joinExpr);
		m_sjMatcher.open(leftGeomColIdx, rightGeomColIdx, srid);
		m_selector = JoinUtils.createJoinColumnSelector(leftSchema, rightSchema, opts.outputColumns());

		Set<String> qkSrc = m_rightClusterCache.getClusterKeyAll();
		QuadKeyBinder qkBinder = new QuadKeyBinder(qkSrc, false);

		m_output = left.flatMap(rec -> {
							Geometry geom = rec.getGeometry(leftGeomColIdx);
							if ( geom == null || geom.isEmpty() ) {
								return FStream.empty();
							}
							
							Envelope envl84 = m_sjMatcher.toMatchKey(geom);
							return FStream.from(qkBinder.bindQuadKeys(envl84))
											.map(b -> KeyValue.of(b.quadkey(), rec));
						})
						.toKeyValueStream(kv -> kv)
						.findBiggestGroupWithinWindow(MAX_WINDOW_SIZE, MIN_WINDOW_SIZE)
						.flatMap(kv -> joinWithOuterGroup(kv.key(), kv.value()));
	}

	@Override
	protected void closeInGuard() throws Exception {
		m_sjMatcher.close();
		m_rightClusterCache.cleanUp();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_selector.getRecordSchema();
	}
	
	@Override
	public Record nextCopy() {
		return m_output.next()
						.ifPresent(r -> ++m_outputCount)
						.getOrNull();
	}
	
	@Override
	public void reportProgress(Logger logger, StopWatch elapsed) {
		if ( !isClosed() || !m_finalProgressReported ) {
			if ( m_input != null &&  m_input instanceof ProgressReportable ) {
				((ProgressReportable)m_input).reportProgress(logger, elapsed);
			}
			
			m_elapsed = elapsed.getElapsedInMillis();
			logger.info("report: [{}]{}", isClosed() ? "C": "O", toString());
			
			if ( isClosed() ) {
				m_finalProgressReported = true;
			}
		}
	}
	
	@Override
	public String toString() {
		double velo = m_leftRecCount / (m_elapsed/1000.0);
		
		return String.format("SpatialBlockJoin: input=%d, output=%d, cluster_io=%d, velo=%.0f/s",
							m_leftRecCount, m_outputCount, m_rightClusterCache.getLoadCount(), velo);
	}
	
	public static RecordSchema calcRecordSchema(RecordSchema leftSchema, RecordSchema rightSchema,
												SpatialJoinOptions opts) {
		return JoinUtils.createJoinColumnSelector(leftSchema, rightSchema, opts.outputColumns())
						.getRecordSchema();
	}
	
	private FStream<Record> joinWithOuterGroup(String quadKey, List<Record> lefts) {
		if ( m_cluster == null || !quadKey.equals(m_cluster.getQuadKey()) ) {
			m_cluster = m_rightClusterCache.getCluster(quadKey);

			if ( s_logger.isInfoEnabled() ) {
				String szStr = UnitUtils.toByteSizeString(m_cluster.length());
				s_logger.info("loaded a {}: {}({}, {}), outer_part={}, in={}, out={}, load_count={}, cache_size={}",
								m_cluster.getClass().getSimpleName(), m_cluster.getQuadKey(),
								m_cluster.getRecordCount(), szStr, lefts.size(),
								m_leftRecCount, m_outputCount,
								m_rightClusterCache.getLoadCount(), m_rightClusterCache.size());
			}
		}
		else if ( quadKey.equals(m_cluster.getQuadKey()) ) {
			if ( s_logger.isDebugEnabled() ) {
				s_logger.debug("reuse the loaded QuadCluster: {}, in={}, out={}, load_count={}, cacheds={}",
								m_cluster.getQuadKey(), m_leftRecCount, m_outputCount,
								m_rightClusterCache.getLoadCount(),
								m_rightClusterCache.getCachedClusterKeyAll());
			}
		}
		
		return FStream.from(lefts)
					.flatMap(outer -> combineInners(outer, m_sjMatcher.match(outer, m_cluster)
																		.map(EnvelopeTaggedRecord::getRecord)));
	}
	
	private FStream<Record> combineInners(Record left, FStream<Record> rights) {
		m_binding.put("left", left);
		++m_leftRecCount;
		return rights.map(inner -> {
			m_binding.put("right", inner);
			return m_selector.select(m_binding);
		});
	}
}