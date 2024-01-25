package marmot.geo.javageom;

import java.util.List;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface OgcMultiGeometry extends OgcGeometry {
	public List<? extends OgcGeometry> getComponents();
}
