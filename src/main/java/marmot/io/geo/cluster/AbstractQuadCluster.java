package marmot.io.geo.cluster;

import org.locationtech.jts.geom.Envelope;

import marmot.GRecordSchema;
import marmot.type.MapTile;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractQuadCluster implements QuadCluster {
	private final String m_quadKey;
	private final boolean m_isOutlier;
	private final GRecordSchema m_gschema;
	private final Envelope m_quadBounds;

	protected AbstractQuadCluster(String quadKey, GRecordSchema gschema) {
		m_quadKey = quadKey;
		m_isOutlier = quadKey.equals("outliers");
		m_gschema = gschema;
		
		m_quadBounds = (m_isOutlier) ? new Envelope() : MapTile.fromQuadKey(quadKey).getBounds();
	}

	@Override
	public String getQuadKey() {
		return m_quadKey;
	}
	
	public boolean isOutlier() {
		return m_isOutlier;
	}

	@Override
	public GRecordSchema getGRecordSchema() {
		return m_gschema;
	}

	@Override
	public Envelope getQuadBounds() {
		return m_quadBounds;
	}
	
	protected boolean isOwnerOf(Envelope envl84) {
		return m_quadBounds.contains(envl84.centre());
	}
}
