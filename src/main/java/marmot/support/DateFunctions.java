package marmot.support;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import utils.Utilities;
import utils.script.MVELFunction;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DateFunctions {
	@MVELFunction(name="DateNow")
	public static LocalDate now() {
		return LocalDate.now();
	}

	@MVELFunction(name="DateFromObject")
	public static LocalDate DateFromObject(Object obj) {
		return asLocalDate(obj);
	}

	@MVELFunction(name="DateYear")
	public static int DateYear(Object obj) {
		return asLocalDate(obj).getYear();
	}

	@MVELFunction(name="DateMonthValue")
	public static int DateMonthValue(Object obj) {
		return asLocalDate(obj).getMonthValue();
	}

	@MVELFunction(name="DateDayOfMonth")
	public static int DateDayOfMonth(Object obj) {
		return asLocalDate(obj).getDayOfMonth();
	}

	@MVELFunction(name="DateWeekDay")
	public static int DateWeekDay(Object obj) {
		return asLocalDate(obj).getDayOfWeek().getValue();
	}

	@MVELFunction(name="DateIsEqual")
	public static boolean DateIsEqual(Object left, Object right) {
		return asLocalDate(left).isEqual(asLocalDate(right));
	}

	@MVELFunction(name="DateIsAfter")
	public static boolean DateIsAfter(Object left, Object right) {
		return  asLocalDate(left).isAfter(asLocalDate(right));
	}

	@MVELFunction(name="DateIsBefore")
	public static boolean DateIsBefore(Object left, Object right) {
		return  asLocalDate(left).isBefore(asLocalDate(right));
	}

	@MVELFunction(name="DateFromMillis")
	public static LocalDate DateFromMillis(long millis) {
		return Utilities.fromUTCEpocMillis(millis).toLocalDate();
	}
	
	@MVELFunction(name="DateToMillis")
	public static long DateToMillis(Object obj) {
		LocalDate date = asLocalDate(obj);
		
		return DateTimeFunctions.DateTimeToMillis(date.atStartOfDay());
	}
	
	@MVELFunction(name="DateFromString")
	public static LocalDate DateFromString(String str) {
		return LocalDate.parse(str);
	}
	
	@MVELFunction(name="DateToString")
	public static String DateToString(Object obj) {
		return asLocalDate(obj).toString();
	}

	@MVELFunction(name="DateParse")
	public static LocalDate parse(String dtStr, String pattern) {
		try {
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
			return LocalDate.parse(dtStr, formatter);
		}
		catch ( Exception e ) {
			return null;
		}
	}

	@MVELFunction(name="DateParseLE")
	public static LocalDate DateParseLE(String dtStr, DateTimeFormatter formatter) {
		try {
			return LocalDate.parse(dtStr, formatter);
		}
		catch ( Exception e ) {
			return null;
		}
	}

	@MVELFunction(name="DatePattern")
	public static DateTimeFormatter DatePattern(String patternStr) {
		return DateTimeFormatter.ofPattern(patternStr);
	}

	@MVELFunction(name="DateFormat")
	public static String DateFormat(Object obj, String pattern) {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
		return asLocalDate(obj).format(formatter);
	}

	@MVELFunction(name="DateToDateTime")
	public static LocalDateTime DateToDateTime(Object obj) {
		return asLocalDate(obj).atStartOfDay();
	}

	@MVELFunction(name="DateDaysBetween")
	public static long DateDaysBetween(Object date1, Object date2) {
		long gap = ChronoUnit.DAYS.between(asLocalDate(date1), asLocalDate(date2));
		return gap;
	}
	
	public static LocalDate asLocalDate(Object obj) {
		if ( obj == null ) {
			return null;
		}
		if ( obj instanceof LocalDate ) {
			return (LocalDate)obj;
		}
		else if ( obj instanceof Date ) {
			return ((Date)obj).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		}
		else if ( obj instanceof LocalDateTime ) {
			return ((LocalDateTime)obj).toLocalDate();
		}
		else {
			throw new IllegalArgumentException("Not Date object: obj=" + obj);
		}
	}
}
