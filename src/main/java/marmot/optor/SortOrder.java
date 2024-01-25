package marmot.optor;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public enum SortOrder {
	/**
	 * Ascending order
	 */
	ASC,
	/**
	 * Descending order
	 */
	DESC,
	NONE;
	
	public static SortOrder fromString(String str) {
		switch ( str.toUpperCase() ) {
			case "ASC":
			case "A":
				return ASC;
			case "DESC":
			case "D":
				return DESC;
			case "NONE":
				return NONE;
			default:
				throw new IllegalArgumentException("invalid SortOrder: " + str);
		}
	}
	
	public static SortOrder fromOrdinal(int ordinal) {
		return values()[ordinal];
	}
}
