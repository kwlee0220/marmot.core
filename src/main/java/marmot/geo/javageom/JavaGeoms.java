package marmot.geo.javageom;

import java.util.List;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.geo.GeoClientUtils;
import math.geom2d.Point2D;
import math.geom2d.polygon.LinearRing2D;
import math.geom2d.polygon.MultiPolygon2D;
import math.geom2d.polygon.Polygon2D;
import math.geom2d.polygon.Polygons2D;
import math.geom2d.polygon.SimplePolygon2D;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JavaGeoms {
	private static final Logger s_logger = LoggerFactory.getLogger(JavaGeoms.class);
	private static final GeometryFactory GEOM_FACT = GeoClientUtils.GEOM_FACT;
	
	public static Geometry intersection(Geometry geom1, Geometry geom2) {
		if ( !(geom1 instanceof MultiPolygon || geom1 instanceof Polygon) ) {
			return null;
		}
		if ( !(geom2 instanceof MultiPolygon || geom2 instanceof Polygon) ) {
			return null;
		}
		
		Polygon2D poly2d1 = toPolygon2D(geom1);
		Polygon2D poly2d2 = toPolygon2D(geom2);
		Polygon2D result = Polygons2D.intersection(poly2d1, poly2d2);
		
		return toGeometry(result);
	}
	
	public static Geometry difference(Geometry geom1, Geometry geom2) {
		if ( !(geom1 instanceof MultiPolygon || geom1 instanceof Polygon) ) {
			return null;
		}
		if ( !(geom2 instanceof MultiPolygon || geom2 instanceof Polygon) ) {
			return null;
		}
		
		Polygon2D poly2d1 = toPolygon2D(geom1);
		Polygon2D poly2d2 = toPolygon2D(geom2);
		Polygon2D result = Polygons2D.difference(poly2d1, poly2d2);
		
		return toGeometry(result);
	}
	
	private static Geometry toGeometry(Polygon2D poly2d) {
		if ( poly2d instanceof MultiPolygon2D ) {
			MultiPolygon2D mp2d = (MultiPolygon2D)poly2d;
			
			Polygon[] polys = new Polygon[mp2d.ringNumber()];
			for ( int i =0; i < mp2d.ringNumber(); ++i ) {
				polys[i] = toPolygon(mp2d.getRing(i));
			}
			return GeoClientUtils.toMultiPolygon(polys);
		}
		else if ( poly2d instanceof SimplePolygon2D ) {
			return toPolygon(((SimplePolygon2D)poly2d).getRing());
		}
		else {
			throw new AssertionError();
		}
	}
	
	private static MultiPolygon2D toMultiPolygon2D(MultiPolygon mpoly) {
		List<LinearRing2D> rings = GeoClientUtils.fstream(mpoly)
												.cast(Polygon.class)
												.map(JavaGeoms::toLinearRing2D)
												.toList();
		return new MultiPolygon2D(rings);
	}
	
	private static LinearRing2D toLinearRing2D(Polygon poly) {
		if ( poly.getNumInteriorRing() > 0 ) {
			throw new JavaGeomException("JavaGeoms cannot support inernal hole");
		}
		
		List<Point2D> ptList = FStream.of(poly.getExteriorRing().getCoordinates())
										.map(coord -> new Point2D(coord.x,coord.y))
										.toList();
		return new LinearRing2D(ptList);
	}
	
	private static SimplePolygon2D toSimplePolygon2D(Polygon poly) {
		return new SimplePolygon2D(toLinearRing2D(poly));
	}
	
	private static Polygon2D toPolygon2D(Geometry geom) {
		switch ( Geometries.get(geom) ) {
			case POLYGON:
				return toSimplePolygon2D((Polygon)geom);
			case MULTIPOLYGON:
				return toMultiPolygon2D((MultiPolygon)geom);
			default:
				return null;
		}
	}
	
	private static Polygon toPolygon(LinearRing2D ring2d) {
		List<Coordinate> coords = FStream.from(ring2d.vertices())
										.map(p2d -> new Coordinate(p2d.getX(), p2d.getY()))
										.toList();
		coords.add(coords.get(0));
		
		return GEOM_FACT.createPolygon(coords.toArray(new Coordinate[coords.size()]));
	}
}
