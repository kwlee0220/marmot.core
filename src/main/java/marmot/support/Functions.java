package marmot.support;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import utils.CSV;
import utils.script.MVELFunction;
import utils.stream.DoubleFStream;
import utils.stream.FStream;
import utils.stream.FloatFStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Functions {
	@MVELFunction(name="ST_StartsWith")
	public static Object startsWith(Object ostr, Object oprefix) {
		return DataUtils.asString(ostr).startsWith(DataUtils.asString(oprefix));
	}
	
	@MVELFunction(name="ST_ArraySum")
	public static Object sumArray(Object array) {
		if ( array instanceof Double[] ) {
			return DoubleFStream.of((Double[])array)
								.fold(0d, (a,v) -> a + v);
		}
		else if ( array instanceof Float[] ) {
			return FloatFStream.of((Float[])array)
								.fold(0d, (a,v) -> a + v);
		}
		else {
			throw new IllegalArgumentException("unsupported array object: " + array);
		}
	}
	@MVELFunction(name="ST_ArrayAvg")
	public static double avgArray(Object array) {
		if ( array instanceof Double[] ) {
			Double[] darr = (Double[])array;
			return DoubleFStream.of(darr)
								.fold(0d, (a,v) -> a + v) / darr.length;
		}
		else if ( array instanceof Float[] ) {
			Float[] farr = (Float[])array;
			return FloatFStream.of(farr)
								.fold(0d, (a,v) -> a + v) / farr.length;
		}
		else {
			throw new IllegalArgumentException("unsupported array object: " + array);
		}
	}
	
	@MVELFunction(name="ST_StringToCsv")
	public static String ST_StringToCsv(Collection<String> coll, String delim) {
		if ( coll != null ) {
			return FStream.from(coll).join(delim);
		}
		else {
			return null;
		}
	}
	
	@MVELFunction(name="ST_StringFromCsv")
	public static List<String> ST_StringFromCsv(String csv, String delim) {
		if ( csv != null ) {
			return CSV.parseCsv(csv, delim.charAt(0), '\\').toList();
		}
		else {
			return null;
		}
	}
	
	@MVELFunction(name="ST_UnionCsv")
	public static String ST_UnionCsv(String csv1, String csv2, String delimStr) {
		char delim = delimStr.charAt(0);
		Set<String> set1 = (csv1 != null) ? CSV.parseCsv(csv1, delim).toSet() : null;
		Set<String> set2 = (csv2 != null) ? CSV.parseCsv(csv2, delim).toSet() : null;
		
		if ( set1 != null && set2 != null ) {
			set1.addAll(set2);
			return FStream.from(set1).join(delim);
		}
		else if ( set1 != null ) {
			return FStream.from(set1).join(delim);
		}
		else if ( set2 != null ) {
			return FStream.from(set2).join(delim);
		}
		else {
			return null;
		}
	}
	
	@MVELFunction(name="Round")
	public static Object Round(Object obj, int digits) {
		if ( obj instanceof Double ) {
			double scale = Math.pow(10, digits);
			return Math.round(((Double)obj) * scale) / scale;
		}
		else if ( obj instanceof Float ) {
			float scale = (float)Math.pow(10, digits);
			return Math.round(((Float)obj) * scale) / scale;
		}
		else {
			throw new IllegalArgumentException("invalid for round-up value=" + obj);
		}
	}
	
	@MVELFunction(name="ST_ChangeCharSet")
	public static String ST_ChangeCharSet(String str, String srcCharset, String tarCharset) {
		try {
			return new String(str.getBytes(srcCharset), tarCharset);
		}
		catch ( UnsupportedEncodingException e ) {
			return null;
		}
	}
}
