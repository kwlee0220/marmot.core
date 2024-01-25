package marmot.support;

import java.util.List;
import java.util.stream.Collectors;

import utils.script.MVELFunction;



/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MVELFunctions {
	@MVELFunction(name="avoid_null")
	public static Object avoidNull(Object value, Object alternative) {
		return ( value != null ) ? value : alternative; 
	}

	@MVELFunction(name="to_csv")
	public static String toCsv(List<String> list) {
		return list.stream().collect(Collectors.joining(","));
	}

	@MVELFunction(name="unwrap_quote")
	public static String unwrapQuote(String str) {
//		if ( str.length() == 2 ) {
//			return "";
//		}
//		else {
			return str.substring(1, str.length()-1);
//		}
	}
}
