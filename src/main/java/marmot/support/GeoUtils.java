package marmot.support;

import java.util.Collection;
import java.util.List;

import javax.measure.Measure;
import javax.measure.quantity.Length;
import javax.measure.unit.SI;

import org.geotools.geometry.jts.JTS;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryComponentFilter;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;

import com.google.common.collect.Lists;

import marmot.geo.GeoClientUtils;
import marmot.type.DataType;
import utils.func.FOption;
import utils.func.Tuple;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeoUtils extends GeoClientUtils {
	public static Geometry simplifyWithTP(Geometry geom, double distTolerance) {
		return TopologyPreservingSimplifier.simplify(geom, distTolerance);
	}
	
	public static Geometry simplifyWithDP(Geometry geom, double distTolerance) {
		return DouglasPeuckerSimplifier.simplify(geom, distTolerance);
	}

	public static Polygon removeSmallHoles(Polygon poly, double threshold) {
		List<LinearRing> holes = Lists.newArrayList();
		
		int nrings = poly.getNumInteriorRing();
		for ( int i =0; i < nrings; ++i ) {
			LinearRing hole = (LinearRing)poly.getInteriorRingN(i);
			if ( hole.getArea() > threshold ) {
				System.out.println("hole kept: size=" + hole.getArea());
				holes.add(hole);
			}
//			else {
//				System.out.println("hole removed: size=" + hole.getArea());
//			}
		}
		
		return GeoClientUtils.GEOM_FACT.createPolygon((LinearRing)poly.getExteriorRing(),
															holes.toArray(new LinearRing[holes.size()]));
	}
	
	public static Polygon removeHoles(Polygon poly) {
		return GeoClientUtils.GEOM_FACT.createPolygon((LinearRing)poly.getExteriorRing());
	}
	
	public static Geometry intersects(Geometry geom1, Geometry geom2, DataType resultType) {
		Geometry result = geom1.intersection(geom2);
		
		if ( resultType == DataType.MULTI_POLYGON ) {
			result = toMultiPolygon(flatten(result, Polygon.class));
		}
		else if ( resultType == DataType.GEOM_COLLECTION
				|| resultType == DataType.MULTI_POINT ) { }
		else if ( resultType == DataType.MULTI_LINESTRING ) {
			List<LineString> lines = Lists.newArrayList();
			result.apply(new GeometryComponentFilter() {
				@Override
				public void filter(Geometry geom) {
					if ( geom instanceof LineString ) {
						lines.add((LineString)geom);
					}
				}
			});
			result = GEOM_FACT.createMultiLineString(
									lines.toArray(new LineString[lines.size()]));
		}
		else {
			throw new IllegalArgumentException("unexpected result-type: " + resultType);
		}
		
		return result;
	}
	
	public static boolean intersects(Envelope envl1, Envelope envl2) {
		double area = envl1.intersection(envl2).getArea();
		return Double.compare(area, 0) > 0;
	}
	
	public static boolean isIntersectionContainedBy(Envelope envl1, Envelope envl2, Envelope tileBounds) {
		Envelope intersection = envl1.intersection(envl2);
		return Double.compare(intersection.getArea(), 0) > 0
				&& tileBounds.contains(intersection);
	}
	
	public static Point fromLonLat(double lon, double lat) {
		return GEOM_FACT.createPoint(new Coordinate(lon, lat));
	}
	
	public static GeometryCollection toGeometryCollection(Geometry... geomList) {
		return GEOM_FACT.createGeometryCollection(geomList);
	}
	
	public static GeometryCollection toGeometryCollection(Collection<? extends Geometry> geomList) {
		return GEOM_FACT.createGeometryCollection(
											geomList.toArray(new Geometry[geomList.size()]));
	}
	
	public static FStream<Geometry> streamSubComponents(Geometry geom) {
		if ( geom instanceof GeometryCollection ) {
			return new SubComponentStream((GeometryCollection)geom);
		}
		else {
			return FStream.empty();
		}
	}
	
	private static class SubComponentStream implements FStream<Geometry> {
		private final GeometryCollection m_coll;
		private final int m_length;
		private int m_index;
		
		private SubComponentStream(GeometryCollection coll) {
			m_coll = coll;
			m_length = coll.getNumGeometries();
			m_index = 0;
		}

		@Override
		public void close() throws Exception {
			m_index = m_length;
		}

		@Override
		public FOption<Geometry> next() {
			if ( m_index < m_length ) {
				return FOption.of(m_coll.getGeometryN(m_index++));
			}
			else {
				return FOption.empty();
			}
		}
	}
	
	public static int getPNUSido(String pnu) {
		return new PNU(pnu).getSido();
	}
	
	public static double erf(double x) {
		// constants
		final double a1 =  0.254829592;
		final double a2 = -0.284496736;
		final double a3 =  1.421413741;
		final double a4 = -1.453152027;
		final double a5 =  1.061405429;
		final double p  =  0.3275911;
	
		// Save the sign of x
		double sign = 1;
		if (x < 0) {
			sign = -1;
		}
		x = Math.abs(x);
	
		// A&S formula 7.1.26
		double t = 1.0/(1.0 + p*x);
		double y = 1.0 - (((((a5*t + a4)*t) + a3)*t + a2)*t + a1)*t*Math.exp(-x*x);
	
		return sign*y;
	}
	
	public static double toPValue(double zvalue) {
		return (1.0 - erf(Math.abs(zvalue)/Math.sqrt(2)));
	}
	
	public static double getGeodesicDistance(Coordinate pt1, Coordinate pt2,
												CoordinateReferenceSystem crs) {
		try {
			double distance = JTS.orthodromicDistance(pt1, pt2, crs);
			Measure<Length> dist = Measure.valueOf(distance, SI.METRE);
			return dist.doubleValue(SI.METRE);
		}
		catch ( TransformException e ) {
			throw new IllegalArgumentException("fails to calc GeodesicDistance: " + pt1 + ", " + pt2);
		}
	}
	
	public static Polygon makePolygon(Coordinate[] coords) {
		Coordinate centroid = GeoClientUtils.GEOM_FACT.createMultiPoint(coords)
											.getCentroid()
											.getCoordinate();
		return makePolygon(coords, centroid);
	}
	
//	public static Polygon makeConcaveHull(MultiPoint points, double threshold) {
//		return (Polygon)new ConcaveHull(points, threshold).getConcaveHull();
//	}
	
	public static Polygon makePolygon(Coordinate[] coords, Coordinate centroid) {
		List<Tuple<Coordinate,Double>> angles = Lists.newArrayList();
		LineString line0 = toLineString(centroid, coords[0]);
		for ( int i =0; i < coords.length; ++i ) {
			LineString line = toLineString(centroid, coords[i]);
			
			angles.add(Tuple.of(coords[i], calcDegreeBtwLinesTo360(line0, line)));
		}
		angles.sort((t1,t2) -> Double.compare(t1._2, t2._2));
		angles.add(Tuple.of(coords[0], -1.0));
		Coordinate[] sorteds = angles.stream()
										.map(t -> t._1)
										.toArray(sz -> new Coordinate[sz]);
		return GeoClientUtils.GEOM_FACT.createPolygon(sorteds);
	}
	
	public static double calcCosineBtwLines(Coordinate ptJoint, Coordinate pt1, Coordinate pt2) {
		Coordinate v1 = minus(ptJoint, pt1);
		Coordinate v2 = minus(ptJoint, pt2);

		float i1 = (float)norm(v1);
		float i2 = (float)norm(v2);

		Coordinate npt1 = new Coordinate(v1.x/i1, v1.y/i1);
		Coordinate npt2 = new Coordinate(v2.x/i2, v2.y/i2);

		return dot(npt1, npt2);
	}
	
	public static double calcRadianBtwLines(Coordinate ptJoint, Coordinate pt1, Coordinate pt2) {
		return Math.acos(calcCosineBtwLines(ptJoint, pt1, pt2));
	}
	
	public static double calcDegreeBtwLines(Coordinate ptJoint, Coordinate pt1, Coordinate pt2) {
		return Math.toDegrees(calcRadianBtwLines(ptJoint, pt1, pt2));
	}
	
	public static double calcDegreeBtwLinesTo360(Coordinate s1, Coordinate e1, Coordinate s2,
													Coordinate e2) {
	    float angle1 = (float) Math.atan2(e1.y - s1.y, s1.x - e1.x);
	    float angle2 = (float) Math.atan2(e2.y - s2.y, s2.x - e2.x);
	    float calculatedAngle = (float) Math.toDegrees(angle1 - angle2);
	    if (calculatedAngle < 0) calculatedAngle += 360;
	    return calculatedAngle;
	}
	
	public static double calcDegreeBtwLinesTo360(LineString line1, LineString line2) {
		Coordinate s1 = line1.getStartPoint().getCoordinate();
		Coordinate e1 = line1.getEndPoint().getCoordinate();
		Coordinate s2 = line2.getStartPoint().getCoordinate();
		Coordinate e2 = line2.getEndPoint().getCoordinate();
		
	    float angle1 = (float) Math.atan2(e1.y - s1.y, s1.x - e1.x);
	    float angle2 = (float) Math.atan2(e2.y - s2.y, s2.x - e2.x);
	    float calculatedAngle = (float) Math.toDegrees(angle1 - angle2);
	    if (calculatedAngle < 0) calculatedAngle += 360;
	    return calculatedAngle;
	}
	
	public static double gradient(Coordinate from, Coordinate to) {
		Coordinate s1 = new Coordinate(from.x, from.y + 1);
		return calcDegreeBtwLinesTo360(s1, from, from, to);
	}
	
	private static Coordinate minus(Coordinate pt1, Coordinate pt2) {
		return new Coordinate(pt1.x - pt2.x, pt1.y - pt2.y);
	}

	private static double dot(Coordinate pt1, Coordinate pt2) {
        return pt1.x * pt2.x + pt1.y * pt2.y;
    }
	
	private static double norm(Coordinate pt) {
		return Math.sqrt(pt.x*pt.x + pt.y*pt.y);
	}
	
	/**
	 * 'lineString'에 포함된 선분들 중에서 주어진 점과의 거리가 가장 짧은 선분을
	 * 해당 거리와 함께 반환한다.
	 * 만일 {@code lineString}가 empty인 경우는 {@link GeoClientUtils#EMPTY_LINESTRING}을
	 * 반환한다.
	 * 동일한 거리의 두 개이상의 선분이 선택된 경우는 임의의 한 선분이 반환한다.
	 * 
	 * @param pt	기준 점 좌표
	 * @param lineString	대상 선분들의 나열.
	 * @return	가장 짧은 거리의 선분과 해당 선분까지의 거리
	 */
	public static Tuple<LineString,Double> findClosestLine(Point pt, LineString lineString) {
		List<Tuple<LineString,Double>> maxLines
									= FStream.from(breakIntoLines(lineString))
											.map(t -> toLineString(t._1, t._2))
											.map(line -> Tuple.of(line, pt.distance(line)))
											.maxMultiple(Tuple::_2);
		return (!maxLines.isEmpty())
					? maxLines.get(0) : Tuple.of(EMPTY_LINESTRING, -1d);
	}
	
	public static Tuple<Point,Double> findClosestPointOnLine(Point pt, LineString lineString) {
		Tuple<LineString,Double> closest = findClosestLine(pt, lineString);
		if ( closest._1.isEmpty() ) {
			return Tuple.of(EMPTY_POINT, closest._2);
		}
		else {
			Coordinate ptc = pt.getCoordinate();
			Coordinate start = closest._1.getStartPoint().getCoordinate();
			Coordinate end = closest._1.getEndPoint().getCoordinate();
			
			Coordinate cpol = findClosestPointOnLine(ptc, start, end);
			return Tuple.of(toPoint(cpol), closest._2);
		}
	}
	
	public static Coordinate findClosestPointOnLine(Coordinate pt, Coordinate start,
															Coordinate end) {
		if ( start.x == end.x ) {	// 선분이 수직인 경우.
			return new Coordinate(start.x, pt.y);
		}
		else if ( start.y == end.y ) {	// 선분이 수평일 경우
			return new Coordinate(pt.x, start.y);
		}
	
		// 그 외의 경우
			
        // 기울기 m1
        double m1 = (start.y - end.y) / (start.x - end.x);
        // 상수 k1
        double k1 = -m1 * start.x + start.y;

        // 이제 선분 l 을 포함하는 직선의 방정식은 y = m1x + k1 이 구해졌습니다.
        // 남은 것은 점 p 를 지나고 위의 직선과 직교하는 직선의 방정식을 구해 봅시다.
        // 두 직선은 직교하기 때문에 m1 * m2 = -1 입니다.

        // 기울기 m2
        double m2 = -1.0 / m1;
        // p 를 지나기 때문에 yp = m2 * xp + k2 => k2 = yp - m2 * xp
        double k2 = pt.y - m2 * pt.x;

        // 두 직선 y = m1x + k1, y = m2x + k2 의 교점을 구한다
        double x = (k2 - k1) / (m1 - m2);
        double y = m1 * x + k1;
        
        // 구한 점이 선분 위에 있는 지 확인
        if ( x >= Math.min(start.x, end.x) && x <= Math.max(start.x, end.x)
        	&& y >= Math.min(start.y, end.y) && y <= Math.max(start.y, end.y) ) {
        	// 구한 교점이 선분위에 있으면
        	return new Coordinate(x, y);
        }
        // 구한 교점이 선분 위에 없으면 p~p1 또는 p~p2 중 작은 값이 최소 거리임
        else {
        	return Double.compare(start.distance(pt), end.distance(pt)) <= 0
        			? start : end;
        }
	}
	
	public static List<Tuple<Coordinate,Coordinate>> breakIntoLines(LineString line) {
		Coordinate[] coords = line.getCoordinates();
		
		List<Tuple<Coordinate,Coordinate>> lines = Lists.newArrayList();
		for ( int i =0; i < coords.length-1; ++i ) {
			lines.add(Tuple.of(coords[i], coords[i+1]));
		}
		
		return lines;
	}
	
	public static Point findWayPoint(LineString line, double dist) {
		List<Tuple<Coordinate,Coordinate>> segments = breakIntoLines(line);
		
		for ( int i =0; i < segments.size(); ++i ) {
			Tuple<Coordinate,Coordinate> segment = segments.get(i);
			
			double length = segment._1.distance(segment._2);
			if ( Double.compare(length, dist) > 0 ) {
				return toPoint(findWayPoint(segment._1, segment._2, dist));
			}
			dist -= length;
		}
		
		return EMPTY_POINT;
	}

	private static Coordinate findWayPoint(Coordinate begin, Coordinate end, double dist) {
		double x = end.x - begin.x;
		double y = end.y - begin.y;
		
		if ( x == 0 ) {
			return new Coordinate(begin.x, begin.y + dist);
		}
		double a = y / x;
		
		double x2 = Math.sqrt(Math.pow(dist, 2) / (1 + Math.pow(a, 2)));
		if ( x < 0 ) {
			x2 = -x2;
		}
		double y2 = a * x2;
		
		return new Coordinate(x2 + begin.x, y2 + begin.y);
	}
}
