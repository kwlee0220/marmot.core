package marmot.io.geo.cluster;

import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;

import marmot.GRecordSchema;
import utils.Throwables;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class QuadClusterCache implements QuadClusterFile<CacheableQuadCluster> {
	private static final Logger s_logger = LoggerFactory.getLogger(QuadClusterCache.class);
//	private static final long CACHE_SIZE = UnitUtils.parseByteSize("256mb");
	private static final long CACHE_SIZE = 3;

	private final QuadClusterFile<? extends CacheableQuadCluster> m_idxFile;
	private final LoadingCache<String,CacheableQuadCluster> m_cache;
	private int m_loadCount = 0;

	public QuadClusterCache(QuadClusterFile<? extends CacheableQuadCluster> idxFile) {
		this(idxFile, CACHE_SIZE);
	}

	public QuadClusterCache(QuadClusterFile<? extends CacheableQuadCluster> idxFile, long cacheSize) {
		m_idxFile = idxFile;
		m_cache = CacheBuilder.newBuilder()
//							.maximumWeight(cacheSize)
//							.weigher(this::weigh)
							.maximumSize(cacheSize)
							.removalListener(this::onClusterRemoved)
							.build(new CacheLoader<String,CacheableQuadCluster>() {
								@Override
								public CacheableQuadCluster load(String quadKey) throws Exception {
									++m_loadCount;
									return m_idxFile.getCluster(quadKey);
								}
							});
	}
	
	public Set<String> getCachedClusterKeyAll() {
		return m_cache.asMap().keySet();
	}
	
	public void cleanUp() {
		m_cache.cleanUp();
	}

	@Override
	public GRecordSchema getGRecordSchema() {
		return m_idxFile.getGRecordSchema();
	}

	@Override
	public Envelope getDataBounds() {
		return m_idxFile.getDataBounds();
	}

	@Override
	public Envelope getQuadBounds() {
		return m_idxFile.getQuadBounds();
	}

	@Override
	public long getRecordCount() {
		return m_idxFile.getRecordCount();
	}

	@Override
	public long getDuplicateCount() {
		return m_idxFile.getDuplicateCount();
	}

	@Override
	public int getClusterCount() {
		return m_idxFile.getClusterCount();
	}
	
	public long size() {
		return m_cache.size();
	}
	
	public int getLoadCount() {
		return m_loadCount;
	}

	@Override
	public CacheableQuadCluster getCluster(String quadKey) {
		try {
			return m_cache.get(quadKey);
		}
		catch ( ExecutionException e ) {
			throw Throwables.toRuntimeException(Throwables.unwrapThrowable(e));
		}
	}

	@Override
	public Set<String> getClusterKeyAll() {
		return m_idxFile.getClusterKeyAll();
	}

	@Override
	public FStream<String> queryClusterKeys(Envelope range84) {
		return m_idxFile.queryClusterKeys(range84);
	}
	
	private int weigh(String quadKey, CacheableQuadCluster cluster) {
		return cluster.length();
	}
	
	private void onClusterRemoved(RemovalNotification<String,CacheableQuadCluster> noti) {
		String quadKey = noti.getKey();
		s_logger.debug("victim cluster selected: cluster={}", quadKey);
	}
}
