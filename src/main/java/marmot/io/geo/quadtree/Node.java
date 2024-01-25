package marmot.io.geo.quadtree;

import org.locationtech.jts.geom.Envelope;

import com.google.common.base.Preconditions;

import marmot.type.MapTile;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class Node<T extends EnvelopedValue, P extends Partition<T>> {
	private final String m_quadKey;
	private final MapTile m_tile;
	
	public abstract int getValueCount();
	public abstract Envelope getDataBounds();
	
	protected Node(String quadKey) {
		Preconditions.checkArgument(quadKey != null, "Node's quad-key is null");
		
		m_quadKey = quadKey;
		m_tile = MapTile.fromQuadKey(quadKey);
	}
	
	public String getQuadKey() {
		return m_quadKey;
	}
	
	public int getSiblingOrdinal() {
		int length = m_quadKey.length();
		if ( length == 0 ) {
			return 0;
		}
		else if ( length == 1 ) {
			return Integer.parseInt(m_quadKey);
		}
		else {
			return Integer.parseInt(m_quadKey.substring(length-1));
		}
	}
	
	public String getParentQuadKey() {
		int qkLen = m_quadKey.length() - 1;
		if ( qkLen < 0 ) {
			throw new IllegalStateException("do not have any parent quadkey: key=" + m_quadKey);
		}
		
		return m_quadKey.substring(0, qkLen);
	}
	
	public MapTile getTile() {
		return m_tile;
	}
	
	public Envelope getTileBounds() {
		return m_tile.getBounds();
	}
}
