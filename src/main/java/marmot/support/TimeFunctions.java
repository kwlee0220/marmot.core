package marmot.support;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import utils.script.MVELFunction;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TimeFunctions {
	@MVELFunction(name="TimeNow")
	public static LocalTime TimeNow() {
		return LocalTime.now();
	}

	@MVELFunction(name="TimeHour")
	public static int TimeHour(Object obj) {
		return asTime(obj).getHour();
	}

	@MVELFunction(name="TimeMinute")
	public static int TimeMinute(Object obj) {
		return asTime(obj).getMinute();
	}

	@MVELFunction(name="TimeSecond")
	public static int TimeSecond(Object obj) {
		return asTime(obj).getSecond();
	}

	@MVELFunction(name="TimeIsEqual")
	public static boolean TimeIsEqual(Object left, Object right) {
		return asTime(left).equals(asTime(right));
	}

	@MVELFunction(name="TimeIsAfter")
	public static boolean TimeIsAfter(Object left, Object right) {
		return asTime(left).isAfter(asTime(right));
	}

	@MVELFunction(name="TimeIsBefore")
	public static boolean TimeIsBefore(Object left, Object right) {
		return asTime(left).isBefore(asTime(right));
	}
	
	@MVELFunction(name="TimeFromString")
	public static LocalTime TimeFromString(String str) {
		return LocalTime.parse(str);
	}
	
	@MVELFunction(name="TimeToString")
	public static String TimeToString(Object obj) {
		LocalTime date = asTime(obj);
		
		return date.toString();
	}

	@MVELFunction(name="TimeParse")
	public static LocalTime TimeParse(String dtStr, String pattern) {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
		return LocalTime.parse(dtStr, formatter);
	}

	@MVELFunction(name="TimeToString")
	public static String TimeToString(LocalTime date, String pattern) {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
		return date.format(formatter);
	}

	@MVELFunction(name="TimeToDateTime")
	public static LocalDateTime TimeToDateTime(LocalDate date, LocalTime time) {
		return time.atDate(date);
	}
	
	public static LocalTime asTime(Object obj) {
		if ( obj != null && obj instanceof LocalTime ) {
			return (LocalTime)obj;
		}
		else {
			throw new IllegalArgumentException("Not asTime: obj=" + obj);
		}
	}
}
