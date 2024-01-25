package marmot.io.geo.quadtree;

import org.locationtech.jts.geom.Envelope;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface EnvelopedValue {
	public Envelope getEnvelope();
	
	public default boolean intersects(Envelope envl) {
		return getEnvelope().intersects(envl);
	}
	
	public default boolean intersects(EnvelopedValue v) {
		return getEnvelope().intersects(v.getEnvelope());
	}
	
	public default boolean contains(EnvelopedValue v) {
		return getEnvelope().contains(v.getEnvelope());
	}
	
	public default boolean containedBy(EnvelopedValue v) {
		return v.getEnvelope().contains(getEnvelope());
	}
}
