package marmot.support;

import java.time.Duration;
import java.time.LocalDateTime;

import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;

import marmot.type.Interval;
import marmot.type.Trajectory;
import utils.script.MVELFunction;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TrajectoryFunctions {
	@MVELFunction(name="ST_TRLineString")
	public static LineString getLineString(Object obj) {
		Trajectory traj = asTrajectory(obj);
		if ( traj == null ) {
			return null;
		}
		
		return traj.getLineString();
	}
	
	@MVELFunction(name="ST_TRSampleCount")
	public static int getSampleCount(Object obj) {
		Trajectory traj = asTrajectory(obj);
		if ( traj == null ) {
			return 0;
		}
		
		return traj.getSampleCount();
	}
	
	@MVELFunction(name="ST_TRLength")
	public static double getLength(Object obj) {
		Trajectory traj = asTrajectory(obj);
		if ( traj == null ) {
			return -1;
		}
		return traj.getLength();
	}
	
	@MVELFunction(name="ST_TRStartSamplePoint")
	public static Point getStartSamplePoint(Object obj) {
		Trajectory traj = asTrajectory(obj);
		if ( traj == null ) {
			return null;
		}
		return traj.getSample(0).getPoint();
	}
	
	@MVELFunction(name="ST_TREndSamplePoint")
	public static Point getEndSamplePoint(Object obj) {
		Trajectory traj = asTrajectory(obj);
		if ( traj == null ) {
			return null;
		}
		return traj.getSample(traj.getSampleCount()-1).getPoint();
	}

	@MVELFunction(name="ST_TRSamplePointAt")
	public static Point getSamplePoint(Object obj, int idx) {
		Trajectory traj = asTrajectory(obj);
		if ( traj == null ) {
			return null;
		}
		
		return traj.getSample(idx).getPoint();
	}
	
	@MVELFunction(name="ST_TRInterval")
	public static Interval getInterval(Object obj) {
		Trajectory traj = asTrajectory(obj);
		if ( traj == null ) {
			return null;
		}
		
		return traj.getInterval();
	}
	
	@MVELFunction(name="ST_TRDuration")
	public static Duration getDuration(Object obj) {
		Trajectory traj = asTrajectory(obj);
		if ( traj == null ) {
			return null;
		}
		
		return traj.getDuration();
	}
	
	@MVELFunction(name="ST_TRStartSampleTime")
	public static LocalDateTime getStartTime(Object obj) {
		return getSampleTime(obj, 0);
	}
	
	@MVELFunction(name="ST_TRSEndSampleTime")
	public static LocalDateTime getEndTime(Object obj) {
		return getSampleTime(obj, getSampleCount(obj)-1);
	}

	@MVELFunction(name="ST_TRSampleTimeAt")
	public static LocalDateTime getSampleTime(Object obj, int idx) {
		Trajectory traj = asTrajectory(obj);
		if ( traj == null ) {
			return null;
		}
		
		return traj.getSample(idx).getTimestamp();
	}
	
	public static Trajectory asTrajectory(Object traj) {
		if ( traj == null ) {
			return null;
		}
		else if ( traj instanceof Trajectory ) {
			return (Trajectory)traj;
		}
		else {
			throw new IllegalArgumentException("Not Trajectory: obj=" + traj);
		}
	}
}
