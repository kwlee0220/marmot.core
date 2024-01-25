package marmot.io.geo.quadtree;

import java.util.List;

import org.locationtech.jts.geom.Envelope;

import com.google.common.collect.Lists;

import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractPartition<T extends EnvelopedValue> implements Partition<T> {
	private final List<T> m_values = Lists.newArrayList();
	private final Envelope m_bounds = new Envelope();
	
	abstract protected boolean mayOverflow(T value);

	@Override
	public FStream<T> values() {
		return FStream.from(m_values);
	}

	@Override
	public int size() {
		return m_values.size();
	}

	@Override
	public Envelope getBounds() {
		return m_bounds;
	}

	@Override
	public boolean add(T value) {
		if ( mayOverflow(value) ) {
			return false;
		}
		
		m_bounds.expandToInclude(value.getEnvelope());
		m_values.add(value);
		
		return true;
	}
}
