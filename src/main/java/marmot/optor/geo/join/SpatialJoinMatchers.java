package marmot.optor.geo.join;

import marmot.optor.geo.SpatialRelation;
import marmot.optor.geo.SpatialRelation.WithinDistanceRelation;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialJoinMatchers {
	private SpatialJoinMatchers() {
		throw new AssertionError();
	}
	
	public static SpatialJoinMatcher WITHIN_DISTANCE(double distance) {
		return new WithinDistanceJoinMatcher(distance);
	}
	
	public static SpatialJoinMatcher parse(String expr) {
		return from(SpatialRelation.parse(expr));
	}
	
	public static SpatialJoinMatcher from(SpatialRelation relation) {
		if ( relation == SpatialRelation.INTERSECTS ) {
			return new IntersectsJoinMatcher();
		}
		else if ( relation instanceof WithinDistanceRelation ) {
			double distance = ((WithinDistanceRelation)relation).getDistance();
			return WITHIN_DISTANCE(distance);
		}
		else {
			throw new IllegalArgumentException("unsupported SpatialRelation: " + relation);
		}
	}
}
