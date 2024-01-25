package marmot.io.geo.quadtree;

import java.io.DataInput;
import java.io.DataOutput;

import org.locationtech.jts.geom.Envelope;

import marmot.io.serializer.MarmotSerializable;
import marmot.io.serializer.MarmotSerializers;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class Pointer implements EnvelopedValue, MarmotSerializable, Comparable<Pointer> {
	private Envelope m_envl;
	private int m_idx = -1;
	
	public Pointer(Envelope envl, int index) {
		m_envl = envl;
		m_idx = index;
	}
	
	public Pointer() { }

	@Override
	public Envelope getEnvelope() {
		return m_envl;
	}
	
	public int index() {
		return m_idx;
	}
	
	public boolean isNull() {
		return m_idx < 0;
	}
	
	@Override
	public String toString() {
		return String.format("%d:%s", m_idx, m_envl);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || !(obj instanceof Pointer) ) {
			return false;
		}
		
		Pointer other = (Pointer)obj;
		return m_idx == other.m_idx;
	}
	
	@Override
	public int hashCode() {
		return m_idx;
	}

	public static Pointer deserialize(DataInput in) {
		Envelope envl = MarmotSerializers.ENVELOPE.deserialize(in);
		int idx = MarmotSerializers.readVInt(in);
		
		return new Pointer(envl, idx);
	}

	@Override
	public void serialize(DataOutput out) {
		MarmotSerializers.ENVELOPE.serialize(m_envl, out);
		MarmotSerializers.writeVInt(m_idx, out);
	}

	@Override
	public int compareTo(Pointer o) {
		return m_idx - o.m_idx;
	}
}