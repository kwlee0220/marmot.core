package marmot.optor.geo.cluster;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Constants {
	private Constants() {
		throw new AssertionError("Should not be called: class=" + Constants.class);
	}

	public static final String COL_QUAD_KEY = "__quad_key";
	public static final String COL_MBR = "__mbr";
	public static final String QUADKEY_OUTLIER = "outliers";
}
