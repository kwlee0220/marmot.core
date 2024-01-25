package marmot.optor;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public enum NullsOrder {
	FIRST,
	LAST;
	
	public static NullsOrder fromString(String str) {
		switch ( str.toUpperCase() ) {
			case "FIRST":
			case "F":
				return FIRST;
			case "LAST":
			case "L":
				return LAST;
		}
		
		throw new AssertionError();
	}
	
	public static NullsOrder fromOrdinal(int ordinal) {
		return values()[ordinal];
	}
}
