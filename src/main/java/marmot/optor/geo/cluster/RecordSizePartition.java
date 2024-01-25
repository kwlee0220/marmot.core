package marmot.optor.geo.cluster;

import java.util.List;

import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import marmot.io.geo.quadtree.EnvelopedValue;
import marmot.io.geo.quadtree.Partition;
import marmot.optor.geo.cluster.RecordSizePartition.RecordSize;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
final class RecordSizePartition implements Partition<RecordSize> {
	private static final Logger s_logger = LoggerFactory.getLogger(RecordSizePartition.class);
	
	private final long m_maxSize;
	private final List<RecordSize> m_slots = Lists.newArrayList();
	private final Envelope m_bounds = new Envelope();
	private long m_totalSize;
	private boolean m_singleLocation;
	
	RecordSizePartition(long maxSize) {
		m_maxSize = maxSize;
	}

	@Override
	public int size() {
		return m_slots.size();
	}

	@Override
	public Envelope getBounds() {
		return m_bounds;
	}
	
	public long getTotalSize() {
		return m_totalSize;
	}

	@Override
	public FStream<RecordSize> values() {
		return FStream.from(m_slots);
	}

	@Override
	public boolean add(RecordSize data) {
		if ( (m_totalSize + data.m_size >  m_maxSize) && !m_singleLocation ) {
			if ( !(m_singleLocation = isSingleLocation()) ) {
				return false;
			}
			
			s_logger.info("become a single_location_partition: count={}", m_slots.size());
		}

		m_totalSize += data.m_size;
		m_bounds.expandToInclude(data.getEnvelope());
		m_slots.add(data);
		
		return true;
	}
	
	@Override
	public String toString() {
		return String.format("%s: total=%d (%.2f%%), count=%d, is_single=%s", getClass().getSimpleName(),
								m_totalSize, (m_totalSize*100.0 / m_maxSize), size(), m_singleLocation);
	}
	
	private boolean isSingleLocation() {
		Envelope unique = m_slots.get(0).getEnvelope();
		for ( RecordSize rsize: m_slots ) {
			if ( !unique.equals(rsize.getEnvelope()) ) {
				return false;
			}
		}
		
		return true;
	}
	
	static final class RecordSize implements EnvelopedValue {
		private final Envelope m_envl;
		private final int m_size;
		
		RecordSize(Envelope envl, int size) {
			m_envl = envl;
			m_size = size;
		}

		@Override
		public Envelope getEnvelope() {
			return m_envl;
		}
		
		@Override
		public String toString() {
			return String.format("record_size=%d", m_size);
		}
	}
}