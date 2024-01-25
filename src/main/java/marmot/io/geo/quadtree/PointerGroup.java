package marmot.io.geo.quadtree;

import java.io.DataInput;
import java.io.DataOutput;

import org.locationtech.jts.geom.Envelope;

import marmot.io.serializer.MarmotSerializable;
import marmot.io.serializer.MarmotSerializers;
import utils.stream.FStream;
import utils.stream.IntFStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
final class PointerGroup implements EnvelopedValue, MarmotSerializable {
	private final Envelope m_envl;
	private final int[] m_indexes;
	
	PointerGroup(Envelope envl, int[] indexes) {
		m_envl = envl;
		m_indexes = indexes;
	}

	@Override
	public Envelope getEnvelope() {
		return m_envl;
	}
	
	FStream<Pointer> stream() {
		return IntFStream.of(m_indexes)
					.mapToObj(index -> new Pointer(m_envl, index));
	}
	
	public static PointerGroup deserialize(DataInput in) {
		Envelope envl = MarmotSerializers.ENVELOPE.deserialize(in);
		int count = MarmotSerializers.readVInt(in);
		int[] indexes = new int[count];
		for ( int i =0; i < count; ++i ) {
			indexes[i] = MarmotSerializers.readVInt(in);
		}
		
		return new PointerGroup(envl, indexes);
	}

	@Override
	public void serialize(DataOutput out) {
		MarmotSerializers.ENVELOPE.serialize(m_envl, out);
		MarmotSerializers.writeVInt(m_indexes.length, out);
		for ( int i =0; i < m_indexes.length; ++i ) {
			MarmotSerializers.writeVInt(m_indexes[i], out);
		}
	}
}